// Copyright (c) 2013, Pantor Engineering AB
//
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions
// are met:
//
//  * Redistributions of source code must retain the above copyright
//    notice, this list of conditions and the following disclaimer.
//
//  * Redistributions in binary form must reproduce the above
//    copyright notice, this list of conditions and the following
//    disclaimer in the documentation and/or other materials provided
//    with the distribution.
//
//  * Neither the name of Pantor Engineering AB nor the names of its
//    contributors may be used to endorse or promote products derived
//    from this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
// FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
//
// IN NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE LIABLE
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
// OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
// BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
// LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
// USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
// DAMAGE.

package com.pantor.xmit;

import java.io.IOException;
import java.net.DatagramSocket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.lang.Math;
import java.util.UUID;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Logger;

import com.pantor.blink.DefaultObjectModel;
import com.pantor.blink.Client;
import com.pantor.blink.Schema;
import com.pantor.blink.DefaultObsRegistry;
import com.pantor.blink.Dispatcher;
import com.pantor.blink.BlinkException;

import xmit.FlowType;
import xmit.Negotiate;
import xmit.NegotiationResponse;
import xmit.NegotiationReject;
import xmit.Establish;
import xmit.EstablishAck;
import xmit.EstablishReject;
import xmit.Heartbeat;
import xmit.Sequence;
import xmit.Terminate;
import xmit.FinishedSending;
import xmit.RetransmitRequest;
import xmit.Retransmission;

import com.pantor.xmit.SessionEventObserver;

/**
   The {@code Session} class provides a basic Xmit UDP client session.

   <p>It sends and receives messages over Xmit.</p>

   <p>You initiate the session through the {@code initiate} method and
   send messages through the {@code send} method.
   The session object dispatches any received app messages to matching
   observers as added by the {@code addAppObserver} method.</p>

   <p>The session is thread based and can either be started through the
   {@code start} method that will spawn a new thread, or it can be
   integrated more flexible with custom created threads through {@code
   Runnable} the interface.</p>
 */

public final class Session implements Runnable
{
   /**
      Creates a session that will use the specified socket. 
      It will map messages as defined by the specified schemas.

      @param socket a DatagramSocket
      @param schemas a vector of blink schemas
      @param obs a session event observer
      @param keepAliveInterval the client side keep alive interval for
             the xmit session
      @param verbosity the amount of logging (0=none)
      @throws XmitException if there is an Xmit problem
      @throws IOException if there is a socket problem
    */

   public Session (DatagramSocket socket,
                   String [] schemas,
                   SessionEventObserver obs,
                   int keepAliveInterval,
                   int verbosity) throws IOException, XmitException
   {
      this.obs = obs;
      this.keepAliveInterval = keepAliveInterval;
      this.verbosity = verbosity;
      this.negotiated = false;
      this.nextSeqNo = 1;
      this.timerTask = null;
      this.lastMsgReceivedTsp = 0;
      this.lastMsgSentTsp = 0;
      this.serverKeepAliveInterval = 0;

      try
      {
         DefaultObjectModel om = new DefaultObjectModel (schemas);
         client = new Client (socket, om);
         client.addObserver (this);

         appRegistry = new DefaultObsRegistry (om);
         appDispatcher = new Dispatcher (om, appRegistry);
      }
      catch (BlinkException e)
      {
         throw new XmitException ("BlinkException: " + e.getMessage ());
      }
   }

   /**
      Adds an observer for received application messages. 
      The prefix when looking up matching observer methods will be "on".

      @param obs an observer to add
      @throws XmitException if there is a schema or binding problem
   */
   
   public void addAppObserver (Object obs) throws XmitException
   {
      try
      {
         appRegistry.addObserver (obs);
      }
      catch (BlinkException e)
      {
         throw new XmitException ("BlinkException: " + e.getMessage ());
      }
   }

   /**
      Initiate an xmit session using optional credentials

      @param credentials optional credentials to use when initiating
      @throws XmitException if there is a schema or binding problem
      @throws IOException if there is a socket problem
   */
   
   public void initiate (Object credentials) throws IOException, XmitException
   {
      if (verbosity > 0)
         log.info ("=> Initiate");

      this.credentials = credentials;

      if (! negotiated)
         negotiate ();
      else
         establish ();
   }

   /**
      Terminate an xmit session

      @param reason the reason for terminating
      @throws XmitException if there is a schema or binding problem
      @throws IOException if there is a socket problem
   */
   
   public void terminate (String reason) throws IOException, XmitException
   {
      if (verbosity > 0)
         log.info ("=> Terminate");

      hbtTask.cancel ();
      hbtTask = null;

      Terminate t = new Terminate ();
      t.setSessionId (sessionId);
      if (! reason.isEmpty ())
         t.setReason (reason);

      try
      {
         client.send (t);
      }
      catch (BlinkException e)
      {
         throw new XmitException ("BlinkException: " + e.getMessage ());
      }
   }

   /**
      Send a message

      @param obj message to send
      @throws XmitException if there is a schema or binding problem
      @throws IOException if there is a socket problem
   */
   
   public void send (Object obj) throws IOException, XmitException
   {
      if (verbosity > 0)
         log.info ("=> Sending " + obj);

      lastMsgSentTsp = System.currentTimeMillis ();
      
      try
      {
         client.send (obj);
      }
      catch (BlinkException e)
      {
         e.printStackTrace ();
         throw new XmitException ("BlinkException: " + e.getMessage ());
      }
   }

   /**
      Runs the session
    */
   
   public void run ()
   {
      try
      {
         client.readLoop ();
      }
      catch (Throwable e)
      {
         e.printStackTrace ();
         while (e.getCause () != null)
            e = e.getCause ();
         log.severe (e.toString ());
      }
   }

   /** Starts the client by creating a new thread */

   public void start ()
   {
      new Thread (this).start ();
   }

   //
   // Observer methods
   //

   public void onNegotiationResponse (NegotiationResponse obj)
      throws IOException, XmitException
   {
      if (obj.getRequestTimestamp () != tsp)
         throw new XmitException ("Negotiation response does not match " +
                                  obj.getRequestTimestamp ());

      if (verbosity > 0)
         log.info ("<= NegotiationResponse");

      negotiated = true;

      // Cancel timer
      timerTask.cancel ();
      timerTask = null;

      establish ();
   }

   public void onNegotiationReject (NegotiationReject obj)
   {
      if (verbosity > 0)
         log.info ("<= NegotiationReject: " + obj.getReason ());

      // Cancel timer
      timerTask.cancel ();
      timerTask = null;

      obs.onRejected (obj.getReason ());
   }

   public void onEstablishAck (EstablishAck obj)
      throws IOException, XmitException
   {
      if (obj.getRequestTimestamp () != tsp)
         throw new XmitException ("Establish response does not match " +
                                  obj.getRequestTimestamp ());

      if (verbosity > 0)
         log.info ("<= EstablishAck");

      if (obj.hasNextSeqNo () && obj.getNextSeqNo () != nextSeqNo)
      {
         System.err.println ("NextSeqNo=" + obj.getNextSeqNo ());
      }

      // Cancel timer
      timerTask.cancel ();
      timerTask = null;

      serverKeepAliveInterval = (int) obj.getKeepaliveInterval ();

      obs.onEstablished (this);

      hbtTask = new HeartbeatTimerTask (this);

      int interval = Math.min (keepAliveInterval, serverKeepAliveInterval);
      timer.schedule (hbtTask, interval, interval);
   }

   public void onEstablishReject (EstablishReject obj)
   {
      if (verbosity > 0)
         log.info ("<= EstablishReject: " + obj.getReason ());

      // Cancel timer
      timerTask.cancel ();
      timerTask = null;

      obs.onRejected (obj.getReason ());
   }

   public void onHeartbeat (Heartbeat obj)
   {
      if (verbosity > 0)
         log.info ("<= Heartbeat");

      lastMsgReceivedTsp = System.currentTimeMillis ();
   }

   public void onSequence (Sequence obj)
   {
      if (verbosity > 0)
         log.info ("<= Sequence");
   }

   public void onRetransmission (Retransmission obj)
   {
      if (verbosity > 0)
         log.info ("<= Restransmission");
   }

   public void onRetransmitRequest (RetransmitRequest obj)
   {
      if (verbosity > 0)
         log.info ("<= RetransmitRequest");
   }

   public void onTerminate (Terminate obj)
   {
      if (verbosity > 0)
         log.info ("<= Terminate");

      hbtTask.cancel ();
      hbtTask = null;
   }

   public void onFinishedSending (FinishedSending obj)
   {
      if (verbosity > 0)
         log.info ("<= FinishedSending");
   }

   public void onAny (Object o)
   {
      if (verbosity > 0)
         log.info ("<= Any " + o.toString ());

      lastMsgReceivedTsp = System.currentTimeMillis ();
      
      try
      {
         appDispatcher.dispatch (o);
      }
      catch (BlinkException e)
      {
         e.printStackTrace ();
      }
   }

   //
   // Private
   //

   private void negotiate () throws IOException, XmitException
   {
      sessionId = new byte [16];

      UUID uuid = UUID.randomUUID ();
      ByteBuffer bb = ByteBuffer.wrap(sessionId);
//      bb.order(ByteOrder.LITTLE_ENDIAN);
      bb.putLong(uuid.getMostSignificantBits());
      bb.putLong(uuid.getLeastSignificantBits());

      tsp = System.currentTimeMillis () * 1000000;

      Negotiate n = new Negotiate ();
      n.setSessionId (sessionId);
      n.setTimestamp (tsp);
      n.setClientFlow (FlowType.Unsequenced);
      n.setCredentials (credentials);

      try
      {
         client.send (n);
      }
      catch (BlinkException e)
      {
         throw new XmitException ("BlinkException: " + e.getMessage ());
      }

      timerTask = new Session.NegotiateTimerTask (this);

      timer.schedule (timerTask, 2000);
   }

   private void establish () throws IOException, XmitException
   {
      if (verbosity > 0)
         log.info ("=> Establish");

      tsp = System.currentTimeMillis () * 1000000;

      Establish m = new Establish ();

      m.setTimestamp (tsp);
      m.setSessionId (sessionId);
      m.setKeepaliveInterval (keepAliveInterval);
      m.setCredentials (credentials);

      try
      {
         client.send (m);
      }
      catch (BlinkException e)
      {
         throw new XmitException ("BlinkException: " + e.getMessage ());
      }

      timerTask = new Session.EstablishTimerTask (this);

      timer.schedule (timerTask, 2000);
   }

   private void onNegotiateTimedOut ()
   {
      try
      {
         log.severe ("No negotiation response, retrying");

         negotiate ();
      }
      catch (Exception e)
      {
         obs.onRejected (e.getMessage ());
      }
   }

   private void onEstablishTimedOut ()
   {
      try
      {
         log.severe ("No establish response, retrying");

         establish ();
      }
      catch (Exception e)
      {
         obs.onRejected (e.getMessage ());
      }
   }

   private void checkHeartbeat ()
   {
      if (verbosity > 0)
         log.info ("Check heartbeat");

      long since = System.currentTimeMillis () - lastMsgReceivedTsp;
      if (since > 3 * serverKeepAliveInterval)
      {
         log.severe ("Heartbeat timed out: " + since
                     + "ms since last msg received");

         try
         {
            terminate ("Heartbeat timeout");
         }
         catch (Exception e)
         {
            // ignored
         }
         
         obs.onRejected ("Heartbeat timeout");
      }
   }

   private void sendHeartbeat ()
   {
      if (verbosity > 0)
         log.info ("=> Heartbeat");

      long since = System.currentTimeMillis () - lastMsgSentTsp;
      if (since < keepAliveInterval)
         return;
      
      try
      {
         Heartbeat hbt = new Heartbeat ();

         client.send (hbt);
      }
      catch (Exception e)
      {
         hbtTask.cancel ();
         hbtTask = null;

         obs.onRejected (e.getMessage ());
      }
   }

   private final class NegotiateTimerTask extends TimerTask
   {
      NegotiateTimerTask (Session s)
      {
         this.session = s;
      }

      public void run ()
      {
         session.onNegotiateTimedOut ();
      }

      Session session;
   }

   private final class EstablishTimerTask extends TimerTask
   {
      EstablishTimerTask (Session s)
      {
         this.session = s;
      }

      public void run ()
      {
         session.onEstablishTimedOut ();
      }

      Session session;
   }

   private final class HeartbeatTimerTask extends TimerTask
   {
      HeartbeatTimerTask (Session s)
      {
         this.session = s;
      }

      public void run ()
      {
         session.sendHeartbeat ();
         session.checkHeartbeat ();
      }

      Session session;
   }

   private final SessionEventObserver obs;
   private final int verbosity;
   private final Client client;
   private final DefaultObsRegistry appRegistry;
   private final Dispatcher appDispatcher;
   private Object credentials;
   private byte[] sessionId;
   private long tsp;
   private long lastMsgReceivedTsp;
   private long lastMsgSentTsp;
   private int keepAliveInterval;
   private int serverKeepAliveInterval;
   private boolean negotiated;
   private long nextSeqNo;
   private TimerTask timerTask;
   private TimerTask hbtTask;
   private static Timer timer = new Timer ();

   private static final Logger log = Logger.getLogger (Client.class.getName ());
}

