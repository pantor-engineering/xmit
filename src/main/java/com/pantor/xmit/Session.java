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
import java.util.ArrayDeque;
import java.util.Timer;
import java.util.Arrays;
import java.util.TimerTask;
import java.util.HashSet;

import com.pantor.blink.DefaultObjectModel;
import com.pantor.blink.Client;
import com.pantor.blink.Schema;
import com.pantor.blink.DefaultObsRegistry;
import com.pantor.blink.Dispatcher;
import com.pantor.blink.BlinkException;
import com.pantor.blink.Logger;

import xmit.FlowType;
import xmit.Negotiate;
import xmit.NegotiationResponse;
import xmit.NegotiationReject;
import xmit.Establish;
import xmit.EstablishmentAck;
import xmit.EstablishmentReject;
import xmit.UnsequencedHeartbeat;
import xmit.Sequence;
import xmit.Context;
import xmit.PackedContext;
import xmit.Terminate;
import xmit.TerminationCode;
import xmit.FinishedSending;
import xmit.RetransmitRequest;
import xmit.Retransmission;
import xmit.Operation;
import xmit.Applied;
import xmit.NotApplied;

/**
   The {@code Session} class provides a basic Xmit UDP client session.

   <p>It sends and receives messages over Xmit.</p>

   <p>You initiate the session through the {@code initiate} method and
   send messages through the {@code send} method.
   The session object dispatches any received app messages to matching
   observers as added by the {@code addAppObserver} method.</p>

   <p>The session is thread based and can either be started through the
   {@code start} method that will spawn a new thread, or it can be
   integrated more flexible with custom created threads through the {@code
   Runnable} interface.</p>
 */

public final class Session implements Runnable, Client.PacketObserver
{
   private final static int CompactArrayLen = 8;
   
   /**
      Creates a session that will use the specified socket. 
      It will map messages as defined by the specified schemas.

      @param socket a DatagramSocket
      @param schemas a vector of blink schemas
      @param obs a session event observer
      @param keepAliveInterval the client side keep alive interval for
             the xmit session
      @throws XmitException if there is an Xmit problem
      @throws IOException if there is a socket problem
    */

   public Session (DatagramSocket socket,
                   String [] schemas,
                   SessionEventObserver obs,
                   int keepAliveInterval) throws IOException, XmitException
   {
      this.obs = obs;
      this.keepAliveInterval = keepAliveInterval;
      this.nextOnceSeqNo = 1;
      this.nextActualIncomingSeqNo = 0;
      this.nextExpectedIncomingSeqNo = 1;
      this.queue = new ArrayDeque<Pending> ();
      this.onceOp = new Operation ();
      this.oncePair = new Object [2];
      this.pendNegReqs = new HashSet<Long> ();
      this.pendEstReqs = new HashSet<Long> ();
      this.pendRetReqs = new HashSet<Long> ();
      this.sessionId = UUID.randomUUID ();
      this.sessionIdBytes = new byte [16];
      
      ByteBuffer bb = ByteBuffer.wrap (sessionIdBytes);
      bb.putLong (sessionId.getMostSignificantBits ());
      bb.putLong (sessionId.getLeastSignificantBits ());
      this.compactSessionIdBytes =
         Arrays.copyOfRange (sessionIdBytes, 0, CompactArrayLen);
      
      oncePair [0] = onceOp;

      try
      {
         DefaultObjectModel om = new DefaultObjectModel (schemas);
         client = new Client (socket, om);
         client.addObserver (this);
         client.setPacketObserver (this);

         appRegistry = new DefaultObsRegistry (om);
         appRegistry.addObserver (new OnceObs ());
         appDispatcher = new Dispatcher (om, appRegistry);
      }
      catch (BlinkException e)
      {
         throw new XmitException (e);
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
         throw new XmitException (e);
      }
   }

   /**
      Initiate an xmit session using optional credentials. Any failure
      to initiate will be reported as failed negotiation or establishment
      through the {@code SessionEventObserver}

      @param credentials credentials to use when initiating. Use
      {@code null} to indicate absence of credentials.
   */
   
   public void initiate (Object credentials)
   {
      log.trace ("=> Initiate");

      this.credentials = credentials;

      if (! negotiated)
         negotiate ();
      else
         establish ();
   }

   /**
      Initiate an xmit session. Any failure to initiate will be
      reported as failed negotiation or establishment through the
      {@code SessionEventObserver}
   */
   
   public void initiate ()
   {
      initiate (null);
   }
   
   /**
      Terminate an xmit session

      @param reason the reason for terminating
   */

   public void terminate (String reason)
   {
      innerTerminate (reason, reason, null, TerminationCode.Finished);
   }

   /**
      Resets the session. If the session is already established, it
      will be terminated.  If there are any ongoing negotiation or
      establishment attempts, they will be aborted. It will however
      retain any already achieved negotiation state.
   */
   
   public void reset ()
   {
      if (established)
         terminate ("reset");
      else
         cancelTimer ();
   }
   
   /**
      Sends an application message

      @param msg message to send
      @throws XmitException if there is a schema or binding problem
      @throws IOException if there is a socket problem
   */
   
   public void send (Object msg) throws IOException, XmitException
   {
      if (log.isActiveAtLevel (Logger.Level.Trace))
         log.trace ("=> Sending " + msg);

      if (! established)
         throw new XmitException ("Session not established");

      try
      {
         innerSend (msg);
      }
      catch (BlinkException e)
      {
         onFailedToSendAppMsg (msg, e);
         throw new XmitException (e);
      }
      catch (IOException e)
      {
         onFailedToSendAppMsg (msg, e);
         throw e;
      }
   }

   /**
      Sends an application message using preceded by an Xmit:Operation
      message to achieve exactly once semantics

      @param msg message to send

      @return the operation seqno assigned to the message
      
      @throws XmitException if there is a schema or binding problem
      @throws IOException if there is a socket problem
   */
   
   public long sendOnce (Object msg) throws IOException, XmitException
   {
      if (log.isActiveAtLevel (Logger.Level.Trace))
         log.trace ("=> Sending " + msg);

      if (! established)
         throw new XmitException ("Session not established");

      try
      {
         return innerSendOnce (msg);
      }
      catch (BlinkException e)
      {
         onFailedToSendAppMsg (msg, e);
         throw new XmitException (e);
      }
      catch (IOException e)
      {
         onFailedToSendAppMsg (msg, e);
         throw e;
      }
   }

   /**
      Sends an application message using preceded by an Xmit:Operation
      message to achieve exactly once semantics

      @param msg message to send
      @param seqNo operation seqNo to use, must be larger than any previously
      used seqnos on this session

      @throws XmitException if there is a schema or binding problem
      @throws IOException if there is a socket problem
   */
   
   public void sendOnce (Object msg, int seqNo)
      throws IOException, XmitException
   {
      if (log.isActiveAtLevel (Logger.Level.Trace))
         log.trace ("=> Sending " + msg);

      if (! established)
         throw new XmitException ("Session not established");

      try
      {
         innerSendOnce (msg, seqNo);
      }
      catch (XmitException e)
      {
         onFailedToSendAppMsg (msg, e);
         throw e;
      }
      catch (BlinkException e)
      {
         onFailedToSendAppMsg (msg, e);
         throw new XmitException (e);
      }
      catch (IOException e)
      {
         onFailedToSendAppMsg (msg, e);
         throw e;
      }
   }

   /**
      Sends an array of messages

      @param msgs messages to send
      @throws XmitException if there is a schema or binding problem
      @throws IOException if there is a socket problem
   */

   public void send (Object [] msgs) throws IOException, XmitException
   {
      send (msgs, 0, msgs.length);
   }

   /**
      Sends a slice of an array of messages

      @param msgs messages to send
      @param from the index of the first message to send
      @param len the number of objects to send
      @throws XmitException if there is a schema or binding problem
      @throws IOException if there is a socket problem
   */

   public void send (Object [] msgs, int from, int len)
      throws IOException, XmitException
   {
      if (log.isActiveAtLevel (Logger.Level.Trace))
         log.trace ("=> Sending " + msgs.length + " messages");

      if (! established)
         throw new XmitException ("Session not established");

      try
      {
         innerSend (msgs, from, len);
      }
      catch (BlinkException e)
      {
         onFailedToSendAppMsg (null, e);
         throw new XmitException (e);
      }
      catch (IOException e)
      {
         onFailedToSendAppMsg (null, e);
         throw e;
      }
   }

   public void send (Iterable<?> msgs) throws IOException, XmitException
   {
      if (log.isActiveAtLevel (Logger.Level.Trace))
         log.trace ("=> Sending messages");

      if (! established)
         throw new XmitException ("Session not established");

      try
      {
         innerSend (msgs);
      }
      catch (BlinkException e)
      {
         onFailedToSendAppMsg (null, e);
         throw new XmitException (e);
      }
      catch (IOException e)
      {
         onFailedToSendAppMsg (null, e);
         throw e;
      }
   }

   /**
      Returns the identifier of this session

      @return a uinque identifier of this session
    */
   
   public UUID getSessionId ()
   {
      return sessionId;
   }
   
   /** Runs the session */

   public void run ()
   {
      try
      {
         client.readLoop ();
      }
      catch (Throwable e)
      {
         log.error (getInnerCause (e), e);
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
   {
      if (log.isActiveAtLevel (Logger.Level.Trace))
         traceResponse (obj, obj.getSessionId (), obj.getRequestTimestamp ());

      if (! negotiated)
      {
         if (isValid (obj))
         {
            receivedMsg ();
            negotiated = true;
            cancelTimer ();
            isSeqSrv = obj.getServerFlow () == FlowType.Sequenced;
            establish ();
         }
      }
      else
      {
         if (isThisSession (obj.getSessionId ()))
            log.warn ("Ignoring negotiation response since " +
                      "already negotiated: " +
                      "resp tsp=" + obj.getRequestTimestamp ());
      }
   }

   public void onNegotiationReject (NegotiationReject obj)
   {
      if (log.isActiveAtLevel (Logger.Level.Trace))
         traceResponse (obj, obj.getSessionId (), obj.getRequestTimestamp ());

      if (! negotiated && isValid (obj))
      {
         receivedMsg ();
         cancelTimer ();
         obs.onNegotiationRejected (getReason (obj));
      }
   }

   public void onEstablishmentAck (EstablishmentAck obj)
      throws XmitException
   {
      if (log.isActiveAtLevel (Logger.Level.Trace))
         traceResponse (obj, obj.getSessionId (), obj.getRequestTimestamp ());

      if (! established)
      {
         if (isValid (obj))            
         {
            receivedMsg ();
            established = true;
            pendTerm = false;
            cancelTimer ();

            serverKeepAliveInterval = obj.getKeepaliveInterval ();

            int interval =
               Math.min (keepAliveInterval, serverKeepAliveInterval * 3);
               
            resetInterval (interval / 8, new TimerTask () {
                  public void run () { onHbtTimer (); }
               });

            obs.onEstablished (this);

            if (obj.hasNextSeqNo ())
               startFrame (obj.getNextSeqNo (),
                           "Xmit:EstablishmentAck.NextSeqNo");
         }
      }
      else
      {
         if (isThisSession (obj.getSessionId ()))
            log.warn ("Ignoring establishment ack since already established: " +
                      "ack tsp=" + obj.getRequestTimestamp ());
      }
   }

   public void onEstablishmentReject (EstablishmentReject obj)
   {
      if (log.isActiveAtLevel (Logger.Level.Trace))
         traceResponse (obj, obj.getSessionId (), obj.getRequestTimestamp ());

      if (! established && isValid (obj))
      {
         receivedMsg ();
         cancelTimer ();
         obs.onEstablishmentRejected (getReason (obj));
      }
   }

   public void onUnsequencedHeartbeat (UnsequencedHeartbeat obj)
   {
      log.trace ("<= UnsequencedHeartbeat");
      if (isEstablished (obj))
         receivedMsg ();
   }

   public void onSequence (Sequence obj)
   {
      log.trace ("<= Sequence");
      if (isEstablished (obj))
      {
         receivedMsg ();
         isRetransmit = false;
         startFrame (obj.getNextSeqNo (), "Xmit:Sequence");
      }
   }

   public void onContext (Context obj)
   {
      if (isThisSession (obj.getSessionId ()))
      {
         if (isEstablished (obj))
         {
            receivedMsg ();
            isRetransmit = false;
            if (obj.hasNextSeqNo ())
               startFrame (obj.getNextSeqNo (), "Xmit:Context.NextSeqNo");
         }
      }
      else
         innerTerminate ("Xmit:Context: Session multiplexing not supported",
                         null, TerminationCode.UnspecifiedError);
   }

   public void onPackedContext (PackedContext obj)
   {
      if (isThisSessionCompact (obj.getSessionIdPrefix ()))
      {
         if (isEstablished (obj))
         {
            receivedMsg ();
            isRetransmit = false;
            if (obj.hasNextSeqNo ())
               startFrame ((long)obj.getNextSeqNo (),
                           "Xmit:PackedContext.NextSeqNo");
         }
      }
      else
         innerTerminate ("Xmit:PackedContext: Session multiplexing not " +
                         "supported", null, TerminationCode.UnspecifiedError);
   }

   public void onRetransmitRequest (RetransmitRequest obj)
   {
      log.trace ("<= RetransmitRequest");
      if (isThisSession (obj.getSessionId ()) && isEstablished (obj))
         innerTerminate ("Xmit:RetransmitRequest not allowed in " +
                         "an unsequenced context", null,
                         TerminationCode.ReRequestOutOfBounds);
   }

   public void onRetransmission (Retransmission obj)
   {
      if (log.isActiveAtLevel (Logger.Level.Trace))
         traceResponse (obj, obj.getSessionId (), obj.getRequestTimestamp ());

      if (isValid (obj) && isEstablished (obj))
      {
         receivedMsg ();
         long nextSeqNo = obj.getNextSeqNo (); 
         long end = nextSeqNo + obj.getCount ();
         if (end >= requestedRetransmitEnd)
            reRequestRetransmitAt = 0;
         else
            reRequestRetransmitAt = end;

         isRetransmit = true;
         startFrame (nextSeqNo, "Xmit:Retransmission");
      }
   }

   public void onTerminate (Terminate obj)
   {
      log.trace ("<= Terminate");
      if (isThisSession (obj.getSessionId ()) && isEstablished (obj))
      {
         String logReason;
         if (obj.hasReason () && ! obj.getReason ().isEmpty ())
            logReason = obj.getReason ();
         else
            logReason = "Terminated by peer: " + obj.getCode ();

         innerTerminate (logReason, "goodbye", null, TerminationCode.Finished);
      }
   }

   public void onFinishedSending (FinishedSending obj)
   {
      log.trace ("<= FinishedSending");
      if (isThisSession (obj.getSessionId ()) && isEstablished (obj))
      {
         isRetransmit = false;
         pendTerm = true;
         if (startFrame (obj.getLastSeqNo () + 1, "Xmit:FinishedSending"))
            flushPendTerm ();
      }
   }

   public void onAny (Object o)
   {
      if (isEstablished (o))
      {
         receivedMsg ();

         if (isSeqSrv)
            onSequencedMsg (o);
         else
         {
            if (log.isActiveAtLevel (Logger.Level.Trace))
               log.trace ("<= Unsequenced app msg: " +
                          o.getClass ().getName ());
            dispatchMsg (o);
         }
      }
   }

   public void onPacketStart ()
   {
      nextActualIncomingSeqNo = 0;
   }
   
   public void onPacketEnd ()
   {
      nextActualIncomingSeqNo = 0;
   }
   
   private void onFailedToSendAppMsg (Object msg, Throwable e)
   {
      if (msg != null)
         innerTerminate ("Failed to send application message: " +
                         msg.getClass ().getName (), e,
                         TerminationCode.UnspecifiedError);
      else
         innerTerminate ("Failed to send an application message", e,
                         TerminationCode.UnspecifiedError);
   }
   
   private static class Pending
   {
      Pending (long seqNo, Object msg)
      {
         this.seqNo = seqNo;
         this.msg = msg;
      }

      long seqNo;
      Object msg;
   }

   private class OnceObs
   {
      public void onApplied (Applied msg)
      {
         obs.onAppliedOnce (msg.getFrom (), msg.getTo ());
      }

      public void onNotApplied (NotApplied msg)
      {
         obs.onNotAppliedOnce (msg.getFrom (), msg.getTo ());
      }
   }

   private boolean startFrame (long nextSeqNo, String what)
   {
      if (log.isActiveAtLevel (Logger.Level.Trace))
         log.trace ("<= NextSeqNo: " + nextSeqNo);
      
      if (isSeqSrv)
      {
         nextActualIncomingSeqNo = nextSeqNo;
         if (nextActualIncomingSeqNo != nextExpectedIncomingSeqNo &&
            ! isRetransmit)
         {
            if (nextActualIncomingSeqNo > nextExpectedIncomingSeqNo)
            {
               recover (nextActualIncomingSeqNo);
               return false;
            }
            else
               log.warn (String.format (
                            what + " comes in too low: seqno %s < %s",
                            nextSeqNo, nextExpectedIncomingSeqNo));
         }
      }
      else
         log.warn ("Received " + what + " for an unsequenced server flow");

      return true;
   }
   
   private void receivedMsg ()
   {
      lastMsgReceivedTsp = now ();
   }
   
   private void sentMsg ()
   {
      lastMsgSentTsp = now ();
   }
   
   private void onSequencedMsg (Object msg)
   {
      if (nextActualIncomingSeqNo != 0)
      {         
         long actualSn = nextActualIncomingSeqNo ++;
         if (log.isActiveAtLevel (Logger.Level.Trace))
            log.trace ("<= Sequenced app msg: " +
                       msg.getClass ().getName () + ", seqNo: " + actualSn);
         
         if (actualSn == nextExpectedIncomingSeqNo)
         {
            ++ nextExpectedIncomingSeqNo;
            dispatchMsg (msg);

            if (firstSeqNoInQueue != 0 &&
                nextExpectedIncomingSeqNo >= firstSeqNoInQueue)
               flushQueue ();
            else
            {
               if (nextExpectedIncomingSeqNo == requestedRetransmitEnd)
                  flushPendTerm ();
            }

            if (nextExpectedIncomingSeqNo == reRequestRetransmitAt)
               requestRetransmit ();
         }
         else
            onOutOfSequenceMsg (actualSn, msg);
      }
      else
         onMissingSequence (msg);
   }

   private void onOutOfSequenceMsg (long actualSn, Object msg)
   {
      if (actualSn > nextExpectedIncomingSeqNo)
      {
         if (! isRetransmit)
            synchronized (queue)
            {
               if (queue.isEmpty ())
                  firstSeqNoInQueue = actualSn;
               queue.add (new Pending (actualSn, msg));
            }
      }
      else
         log.warn (String.format (
                      "Ignoring already seen message %s with seqno %s, " +
                      "next expected seqno is %s", msg.getClass ().getName (),
                      actualSn, nextExpectedIncomingSeqNo));
   }

   private void onMissingSequence (Object msg)
   {
      String reason = "No sequencing message seen when receiving " +
         msg.getClass ().getName ();
      log.error (reason + ", terminating");
      innerTerminate (reason, null, TerminationCode.UnspecifiedError);
   }
   
   private void dispatchMsg (Object msg)
   {
      try
      {
         appDispatcher.dispatch (msg);
      }
      catch (BlinkException e)
      {
         log.warn (getInnerCause (e), e);
      }
   }

   private void recover (long to)
   {
      if (firstSeqNoInQueue != 0 && firstSeqNoInQueue < to)
         requestedRetransmitEnd = firstSeqNoInQueue;
      else
         requestedRetransmitEnd = to;

      log.info (String.format ("Inbound sequence gap detected: " +
                               "expected %s but got %s",
                               nextExpectedIncomingSeqNo, to));

      requestRetransmit ();
   }
   
   private void requestRetransmit ()
   {
      long tsp = nowNano ();
      synchronized (pendRetReqs)
      {
         pendRetReqs.add (tsp);
      }

      RetransmitRequest rr = new RetransmitRequest ();
      rr.setSessionId (sessionIdBytes);
      rr.setTimestamp (tsp);
      rr.setFromSeqNo (nextExpectedIncomingSeqNo);
      rr.setCount ((int) (requestedRetransmitEnd - nextExpectedIncomingSeqNo));

      reRequestRetransmitAt = 0;
      
      try
      {
         innerSend (rr);
      }
      catch (Exception e)
      {
         innerTerminate ("Failed to send retransmit request", e,
                         TerminationCode.UnspecifiedError);
      }
   }

   private void flushQueue ()
   {
      int dequeued = 0;

      synchronized (queue)
      {
         while (! queue.isEmpty ())
         {
            Pending pend = queue.peek ();
            if (pend.seqNo == nextExpectedIncomingSeqNo)
            {
               ++ nextExpectedIncomingSeqNo;
               ++ dequeued;
               dispatchMsg (pend.msg);
               queue.pop ();
            }
            else if (pend.seqNo < nextExpectedIncomingSeqNo)
               queue.pop ();
            else
               break;
         }

         if (! queue.isEmpty ())
            firstSeqNoInQueue = queue.peek ().seqNo;
         else
         {
            firstSeqNoInQueue = 0;
            flushPendTerm ();
         }
      }
      
      if (dequeued > 0 && log.isActiveAtLevel (Logger.Level.Trace))
         log.trace ("dequeued " + dequeued + " after retransmit complete");
   }

   private void flushPendTerm ()
   {
      if (pendTerm)
      {
         pendTerm = false;
         innerTerminate ("peer finished sending", "finished receiving", null,
                         TerminationCode.Finished);            
      }
   }
   
   private void innerTerminate (String logReason, String sendReason,
                                Throwable cause, TerminationCode code)
   {
      log.trace ("=> Terminate: " + logReason);
      if (established)
      {
         try
         {
            established = false;
            cancelTimer ();

            Terminate t = new Terminate ();
            t.setSessionId (sessionIdBytes);
            t.setCode (code);
            if (sendReason != null && ! sendReason.isEmpty ())
               t.setReason (sendReason);

            try
            {
               innerSend (t);
            }
            catch (Exception e)
            {
               log.warn ("Graceful termination failed", e);
            }
         }
         finally
         {
            obs.onTerminated (logReason != null ? logReason : "terminated",
                              cause);
         }
      }
      else
         log.trace ("Terminating not established session");
   }

   private void innerTerminate (String reason, Throwable cause,
                                TerminationCode code)
   {
      innerTerminate (reason, reason, cause, code);
   }

   private synchronized long innerSendOnce (Object msg)
      throws IOException, BlinkException
   {
      sentMsg ();
      int seqNo = nextOnceSeqNo ++;
      onceOp.setSeqNo (seqNo);
      oncePair [1] = msg;
      client.send (oncePair);
      oncePair [1] = null;
      return seqNo;
   }

   private synchronized void innerSendOnce (Object msg, int seqNo)
      throws IOException, BlinkException, XmitException
   {
      if (seqNo >= nextOnceSeqNo)
         nextOnceSeqNo = seqNo + 1;
      else
      {
         String reason =
            "Operation seqno too low: " + seqNo + " < " + nextOnceSeqNo;
         if (seqNo == 0)
            reason += " (seqnos start from 1)";
         throw new XmitException (reason);
      }

      sentMsg ();
      onceOp.setSeqNo (seqNo);
      oncePair [1] = msg;
      client.send (oncePair);
      oncePair [1] = null;
   }

   private synchronized void innerSend (Object msg)
      throws IOException, BlinkException
   {
      sentMsg ();
      client.send (msg);
   }
   
   private synchronized void innerSend (Iterable<?> msgs)
      throws IOException, BlinkException
   {
      sentMsg ();
      client.send (msgs);
   }
   
   private synchronized void innerSend (Object [] msgs, int from, int len)
      throws IOException, BlinkException
   {
      sentMsg ();
      client.send (msgs, from, len);
   }
   
   private boolean isValid (EstablishmentAck obj)
   {
      return isValid (obj.getSessionId (), obj.getRequestTimestamp (),
                      pendEstReqs, "Establishment ack");
   }

   private boolean isValid (EstablishmentReject obj)
   {
      return isValid (obj.getSessionId (), obj.getRequestTimestamp (),
                      pendEstReqs, "Establishment reject");
   }

   private boolean isValid (NegotiationResponse obj)
   {
      return isValid (obj.getSessionId (), obj.getRequestTimestamp (),
                      pendNegReqs, "Negotiation response");
   }

   private boolean isValid (NegotiationReject obj)
   {
      return isValid (obj.getSessionId (), obj.getRequestTimestamp (),
                      pendNegReqs, "Negotiation reject");
   }

   private boolean isValid (Retransmission obj)
   {
      return isValid (obj.getSessionId (), obj.getRequestTimestamp (),
                      pendRetReqs, "Retransmission");
   }

   private boolean isValid (byte [] snId, long reqTsp,
                            HashSet<Long> pendReqs, String what)
   {
      if (isThisSession (snId))
      {
         synchronized (pendReqs)
         {
            if (pendReqs.remove (reqTsp))
               return true;
            else
               log.warn (String.format ("Unknown %s tsp: %s", what, reqTsp));
         }
      }
      else
         log.warn (
            String.format ("%s session id mismatch: expected: %s != actual: %s",
                           what, sessionId, toUuid (snId)));
      
      return false;
   }

   private static String getReason (EstablishmentReject obj)
   {
      if (obj.hasReason () && ! obj.getReason ().isEmpty ())
         return obj.getReason ();
      else
         return obj.getCode ().toString ();
   }
   
   private static String getReason (NegotiationReject obj)
   {
      if (obj.hasReason () && ! obj.getReason ().isEmpty ())
         return obj.getReason ();
      else
         return obj.getCode ().toString ();
   }
   
   private boolean isThisSession (byte [] id)
   {
      return java.util.Arrays.equals (id, sessionIdBytes);
   }

   private boolean isThisSessionCompact (byte [] id)
   {
      return java.util.Arrays.equals (id, compactSessionIdBytes);
   }
   
   private static UUID toUuid (byte [] bytes)
   {
      ByteBuffer bb = ByteBuffer.wrap (bytes);
      long hi = bb.getLong ();
      long lo = bb.getLong ();
      return new UUID (hi, lo);
   }
   
   private synchronized void cancelTimer ()
   {
      if (timerTask != null)
      {
         timerTask.cancel ();
         timerTask = null;
      }
   }

   private synchronized void resetInterval (int interval, TimerTask task)
   {
      cancelTimer ();
      timerInterval = interval;
      timerTask = task;
      timer.schedule (timerTask, interval, interval);
   }
   
   private synchronized void resetTimer (int time, TimerTask task)
   {
      cancelTimer ();
      timerTask = task;
      timer.schedule (timerTask, time);
   }
   
   private static String getInnerCause (Throwable e)
   {
      while (e.getCause () != null)
         e = e.getCause ();
      return e.toString ();
   }

   private final static int NegotiateTimeout = 2000; // Millisecs

   private void negotiate ()
   {
      try
      {
         long tsp = nowNano ();
         synchronized (pendNegReqs)
         {
            pendNegReqs.add (tsp);
         }

         if (log.isActiveAtLevel (Logger.Level.Trace))
            log.trace ("=> Negotiate (tsp " + tsp + ")");

         Negotiate n = new Negotiate ();

         n.setSessionId (sessionIdBytes);
         n.setTimestamp (tsp);
         n.setClientFlow (FlowType.Unsequenced);
         n.setCredentials (credentials);

         innerSend (n);

         resetTimer (NegotiateTimeout, new TimerTask () {
               public void run () { onNegotiationTimedOut (); }
            });
      }
      catch (Exception e)
      {
         obs.onNegotiationFailed (e);
      }
   }

   private final static int EstablishTimeout = 2000; // Millisecs
   
   private void establish ()
   {
      try
      {
         long tsp = nowNano ();
         synchronized (pendEstReqs)
         {
            pendEstReqs.add (tsp);
         }

         if (log.isActiveAtLevel (Logger.Level.Trace))
            log.trace ("=> Establish (tsp " + tsp + ")");

         Establish m = new Establish ();

         m.setSessionId (sessionIdBytes);
         m.setTimestamp (tsp);
         m.setKeepaliveInterval (keepAliveInterval);
         m.setCredentials (credentials);

         innerSend (m);

         resetTimer (EstablishTimeout, new TimerTask () {
               public void run () { onEstablishmentTimedOut (); }
            });
      }
      catch (Exception e)
      {
         obs.onEstablishmentFailed (e);
      }
   }

   private boolean isEstablished (Object msg)
   {
      if (established)
         return true;
      else
      {
         log.warn (msg.getClass ().getName () + " received before establish");
         return false;
      }
   }
   
   private static long now ()
   {
      return System.currentTimeMillis ();
   }

   private static long nowNano ()
   {
      return System.nanoTime ();
   }
   
   private void onNegotiationTimedOut ()
   {
      if (! negotiated)
      {
         log.warn ("No negotiation response, retrying");
         negotiate ();
      }
   }

   private void onEstablishmentTimedOut ()
   {
      if (! established)
      {
         log.warn ("No establish response, retrying");
         establish ();
      }
   }

   private void checkServerIsAlive ()
   {
      log.trace ("Check server is alive");
      long elapsed = now () - lastMsgReceivedTsp;
      if (elapsed > 3 * serverKeepAliveInterval)
         innerTerminate ("Session timed out after receiving no message in: " +
                         elapsed + "ms", null, TerminationCode.Timeout);
   }

   private void showClientIsAlive ()
   {
      log.trace ("Show client is alive");
      long elapsed = now () - lastMsgSentTsp;
      if (elapsed + timerInterval >= (int)(keepAliveInterval * 0.9))
         try
         {
            log.trace ("=> UnsequencedHeartbeat");
            innerSend (new UnsequencedHeartbeat ());
         }
         catch (Exception e)
         {
            innerTerminate ("Failed to send unsequenced heartbeat", e,
                            TerminationCode.UnspecifiedError);
         }
   }

   private void onHbtTimer ()
   {
      showClientIsAlive ();
      checkServerIsAlive ();
   }

   private void traceResponse (Object msg, byte [] snId, long tsp)
   {
      log.trace (String.format ("<= %s (snId: %s, reqTsp: %s)",
                                msg.getClass ().getName (),
                                toUuid (snId), tsp));
   }

   private final SessionEventObserver obs;
   private final Client client;
   private final DefaultObsRegistry appRegistry;
   private final Dispatcher appDispatcher;
   private final ArrayDeque<Pending> queue;
   private final Operation onceOp;
   private final Object [] oncePair;
   private final UUID sessionId;
   private final byte[] sessionIdBytes;
   private final byte[] compactSessionIdBytes;
   private final HashSet<Long> pendNegReqs;
   private final HashSet<Long> pendEstReqs;
   private final HashSet<Long> pendRetReqs;

   private Object credentials;
   
   private volatile int keepAliveInterval;
   private volatile int serverKeepAliveInterval;
   private volatile long retReqTsp;
   private volatile long lastMsgReceivedTsp;
   private volatile long lastMsgSentTsp;
   private volatile boolean negotiated;
   private volatile boolean established;
   private volatile long firstSeqNoInQueue;
   private volatile long nextExpectedIncomingSeqNo;
   private volatile long nextActualIncomingSeqNo;
   private volatile long reRequestRetransmitAt;
   private volatile long requestedRetransmitEnd;
   
   private TimerTask timerTask;
   private int timerInterval;
   private static Timer timer = new Timer ("XmitSessionTimer",
                                           true /* daemon */);
   private boolean isSeqSrv;
   private boolean pendTerm;
   private int nextOnceSeqNo;
   private boolean isRetransmit;

   private final Logger log = Logger.Manager.getLogger (Client.class);
}
