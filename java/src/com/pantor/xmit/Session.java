// Copyright (c), Pantor Engineering AB, 2013-
// All rights reserved

package com.pantor.xmit;

import java.io.IOException;
import java.net.DatagramSocket;
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

public final class Session implements Runnable
{
   public final class NegotiateTimerTask extends TimerTask
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

   public final class EstablishTimerTask extends TimerTask
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

   public final class HeartbeatTimerTask extends TimerTask
   {
      HeartbeatTimerTask (Session s)
      {
	 this.session = s;
      }

      public void run ()
      {
	 session.sendHeartbeat ();
      }

      Session session;
   }

   public Session (DatagramSocket socket,
		   String [] schemas,
		   SessionEventObserver obs,
		   int keepAliveInterval,
		   int verbosity) throws IOException, XmitException
   {
      this.obs = obs;
      this.keepAliveInterval = keepAliveInterval;
      this.verbosity = verbosity;
      negotiated = false;
      nextSeqNo = 1;
      timerTask = null;

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

   public void send (Object obj) throws IOException, XmitException
   {
      if (verbosity > 0)
	 log.info ("=> Sending " + obj);
      
      try
      {
	 client.send (obj);
      }
      catch (BlinkException e)
      {
	 throw new XmitException ("BlinkException: " + e.getMessage ());
      }
   }
   
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

   public void start ()
   {
      new Thread (this).start ();
   }

   //
   // Private
   //

   private void negotiate () throws IOException, XmitException
   {
      sessionId = UUID.randomUUID ().toString ();
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
   
   private void sendHeartbeat ()
   {
      if (verbosity > 0)
	 log.info ("=> Heartbeat");
      
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
      
      obs.onEstablished (this);

      hbtTask = new HeartbeatTimerTask (this);

      timer.schedule (hbtTask, keepAliveInterval, keepAliveInterval);
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
      
      try
      {
	 appDispatcher.dispatch (o);
      }
      catch (BlinkException e)
      {
	 e.printStackTrace ();
      }
   }

   private final SessionEventObserver obs;
   private final int verbosity;
   private final Client client;
   private final DefaultObsRegistry appRegistry;
   private final Dispatcher appDispatcher;
   private Object credentials;
   private String sessionId;
   private long tsp;
   private int keepAliveInterval;
   private boolean negotiated;
   private long nextSeqNo;
   private TimerTask timerTask;
   private TimerTask hbtTask;
   private static Timer timer = new Timer ();

   private static final Logger log = Logger.getLogger (Client.class.getName ());
}

