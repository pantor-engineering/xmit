// Copyright (c) 2014, Pantor Engineering AB
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

package com.pantor.xmit.impl;

import java.io.IOException;
import java.net.DatagramSocket;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.ByteOrder;
import java.lang.Math;
import java.util.UUID;
import java.util.ArrayDeque;
import java.util.Timer;
import java.util.Arrays;
import java.util.TimerTask;
import java.util.HashSet;
import java.util.Date;

import com.pantor.blink.DefaultObsRegistry;
import com.pantor.blink.Dispatcher;
import com.pantor.blink.ObjectModel;
import com.pantor.blink.BlinkException;
import com.pantor.blink.CompactReader;
import com.pantor.blink.CompactWriter;
import com.pantor.blink.ByteBuf;
import com.pantor.blink.NsName;
import com.pantor.blink.Observer;
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
import xmit.Applied;
import xmit.NotApplied;

import com.pantor.xmit.Client.Session.EventObserver;
import com.pantor.xmit.Client;
import com.pantor.xmit.XmitException;

import static com.pantor.xmit.impl.Util.*;

// FIXME: Handle finished sending/receiving

public final class ClientSession implements Client.Session
{
   private final static int Sec = 1000;
   private final static int PackedPrefixLen = 8;
   
   public ClientSession (DatagramChannel ch, ObjectModel om,
                         EventObserver obs, int keepaliveInterval,
                         int opTimeout, Client.FlowType flowType)
      throws IOException, XmitException
   {
      try
      {
         this.flowType = flowType;
         this.obs = obs;
         this.keepaliveInterval = keepaliveInterval;
         this.nextOutgoingSeqNo = 1;
         this.nextActualIncomingSeqNo = 0;
         this.nextExpectedIncomingSeqNo = 1;
         this.pendIncoming = new ArrayDeque<PendMsg> ();
         this.seqPrefix = new Sequence ();
         this.pendNegReqs = new HashSet<Long> ();
         this.pendEstReqs = new HashSet<Long> ();
         this.pendRetReqs = new HashSet<Long> ();
         this.sessionId = UUID.randomUUID ();
         this.sessionIdBytes = toBytes (sessionId);
         this.packedSessionIdBytes =
            Arrays.copyOfRange (sessionIdBytes, 0, PackedPrefixLen);
         this.inBb = ByteBuffer.allocate (1500);
         this.inBuf = new ByteBuf (inBb.array (), 0, inBb.limit ());
         DefaultObsRegistry oreg = new DefaultObsRegistry (om);
         oreg.addObserver (this);
         this.rd = new CompactReader (om, oreg);
         this.outBb = ByteBuffer.allocate (1500);
         this.outBuf = new ByteBuf (outBb.array ());
         this.wr = new CompactWriter (om, outBuf);
         this.isIdempotent = flowType == Client.FlowType.Idempotent;
         this.ch = ch;
         this.appRegistry = new DefaultObsRegistry (om);
         this.appDispatcher = new Dispatcher (om, appRegistry);

         appRegistry.addObserver (new OperationObs ());

         // Set the minimum operation timeout to 3 secs to have at
         // least once second between the head and tail of the pend
         // ops ring. This gives a lower bound of once second on the
         // effective operation timeout
         
         this.opTimeout = Math.max (3 * Sec, opTimeout);
         this.pendOpsSize = this.opTimeout / Sec;
         this.pendOps = new PendOpRange [pendOpsSize];
         for (int i = 0; i < pendOpsSize; ++ i)
            pendOps [i] = new PendOpRange ();
         this.pendResends = new ArrayDeque<PendOpRange> ();
      }
      catch (BlinkException e)
      {
         throw new XmitException (e);
      }
   }

   private static final class PendOpRange
   {
      PendOpRange (long tsp, long from, long end)
      {
         this.tsp = tsp;
         this.from = from;
         this.end = end;
      }

      PendOpRange ()
      {
         this (0, 0, 0);
      }
      
      long tsp;
      long from;
      long end;
   }

   @Override
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

   @Override
   public void addAppObserver (Object obs, String prefix) throws XmitException
   {
      try
      {
         appRegistry.addObserver (obs, prefix);
      }
      catch (BlinkException e)
      {
         throw new XmitException (e);
      }
   }

   @Override
   public void addAppObserver (NsName name, Observer obs)
   {
      appRegistry.addObserver (name, obs);
   }

   @Override
   public void initiate (Object credentials)
   {
      log.trace ("=> Initiate");

      this.credentials = credentials;

      if (! negotiated)
         negotiate ();
      else
         establish ();
   }

   @Override
   public void initiate ()
   {
      initiate (null);
   }
   
   @Override
   public void terminate (String reason)
   {
      innerTerminate (reason, reason, null, TerminationCode.Finished);
   }

   @Override
   public void reset ()
   {
      if (established)
         terminate ("api reset");
      else
         cancelTimer ();
   }

   @Override
   public void reset (String reason)
   {
      if (established)
         terminate (reason);
      else
         cancelTimer ();
   }
   
   @Override
   public long send (Object msg) throws IOException, XmitException
   {
      if (log.isActiveAtLevel (Logger.Level.Trace))
         log.trace ("=> Sending app message %s", getMsgType (msg));

      if (! established)
         throw new XmitException ("Session not established");

      try
      {
         if (isIdempotent)
            return innerSendIdemp (msg);
         else
         {
            innerSend (msg);
            return 0;
         }
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

   @Override
   public long send (Object [] msgs) throws IOException, XmitException
   {
      return send (msgs, 0, msgs.length);
   }

   @Override
   public long send (Object [] msgs, int from, int len)
      throws IOException, XmitException
   {
      if (log.isActiveAtLevel (Logger.Level.Trace))
         log.trace ("=> Sending %d app messages", msgs.length);

      if (! established)
         throw new XmitException ("Session not established");

      try
      {
         if (isIdempotent)
            return innerSendIdemp (msgs, from, len);
         else
         {
            innerSend (msgs, from, len);
            return 0;
         }
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

   @Override
   public long send (Iterable<?> msgs) throws IOException, XmitException
   {
      if (log.isActiveAtLevel (Logger.Level.Trace))
         log.trace ("=> Sending app messages");

      if (! established)
         throw new XmitException ("Session not established");

      try
      {
         if (isIdempotent)
            return innerSendIdemp (msgs);
         else
         {
            innerSend (msgs);
            return 0;
         }
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

   @Override
   public void resend (Object msg, long seqNo) throws IOException, XmitException
   {
      if (log.isActiveAtLevel (Logger.Level.Trace))
         log.trace ("=> Resending app message %s", getMsgType (msg));

      if (! established)
         throw new XmitException ("Session not established");

      try
      {
         resendRequiresIdemp ();
         innerResendIdemp (msg, seqNo);
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
   
   @Override
   public void resend (Object [] msgs, long firstSeqNo)
      throws IOException, XmitException
   {
      resend (msgs, 0, msgs.length, firstSeqNo);
   }

   @Override
   public void resend (Object [] msgs, int from, int len, long firstSeqNo)
      throws IOException, XmitException
   {
      if (log.isActiveAtLevel (Logger.Level.Trace))
         log.trace ("=> Resending %d app messages", msgs.length);

      if (! established)
         throw new XmitException ("Session not established");

      try
      {
         resendRequiresIdemp ();
         innerResendIdemp (msgs, from, len, firstSeqNo);
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

   @Override
   public void resend (Iterable<?> msgs, long firstSeqNo)
      throws IOException, XmitException
   {
      if (log.isActiveAtLevel (Logger.Level.Trace))
         log.trace ("=> Resending app messages");

      if (! established)
         throw new XmitException ("Session not established");

      try
      {
         resendRequiresIdemp ();
         innerResendIdemp (msgs, firstSeqNo);
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
   
   private void resendRequiresIdemp () throws XmitException
   {
      if (! isIdempotent)
         throw new XmitException (
            String.format ("Resend can only be used with the " +
                           "idempotent flow type: %s", flowType));
   }

   @Override
   public UUID getSessionId ()
   {
      return sessionId;
   }
   
   @Override
   public void run ()
   {
      try
      {
         eventLoop ();
      }
      catch (Throwable e)
      {
         log.error (getInnerCause (e), e);
      }
   }

   @Override
   public void start ()
   {
      new Thread (this).start ();
   }

   @Override
   public void stop ()
   {
      if (established)
         innerTerminate ("Session stopped", null, TerminationCode.Finished);
      log.trace ("Session stopped");
      done = true;
   }
   
   @Override
   public void eventLoop () throws XmitException, IOException
   {
      try
      {
         while (! done)
            receivePacket ();
      }
      catch (BlinkException e)
      {
         throw new XmitException (e);
      }
      finally
      {
         ch.close ();
      }
   }

   private void receivePacket () throws BlinkException, IOException
   {
      inBb.clear ();
      ch.receive (inBb);
      inBb.flip ();
      if (inBb.limit () > 0)
      {
         inBuf.clear ();
         inBuf.setPos (inBb.limit ());
         inBuf.flip ();
         decodePacket ();
      }
      else
         log.warn ("%s: Empty packet", info (ch));
   }

   private void decodePacket () throws BlinkException, IOException
   {
      try
      {
         onPacketStart ();
         rd.read (inBuf);
         if (! rd.isComplete ())
            log.warn ("%s: Incomplete Blink content in packet", info (ch));
         onPacketEnd ();
      }
      finally
      {
         rd.reset ();
      }
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
            if (isIdempotent && ! isSeqSrv)
            {
               log.fatal ("Cannot use an idempotent client session with " +
                          "a non-sequenced server, exiting");
               done = true;
            }
            establish ();
         }
      }
      else
      {
         if (isThisSession (obj.getSessionId ()))
            log.warn ("Ignoring negotiation response since " +
                      "already negotiated: " +
                      "req tsp: %s", nanoToStr (obj.getRequestTimestamp ()));
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
      {
         String extra = "";
         if (obj.hasNextSeqNo ())
            extra = String.format (", next seq no: %d", obj.getNextSeqNo ());
         traceResponse (obj, obj.getSessionId (), obj.getRequestTimestamp (),
                        extra);
      }

      if (! established)
      {
         if (isValid (obj))            
         {
            receivedMsg ();
            established = true;
            pendTerm = false;
            cancelTimer ();

            serverKeepaliveInterval = obj.getKeepaliveInterval ();

            int interval =
               Math.min (keepaliveInterval, serverKeepaliveInterval * 3);

            if (isIdempotent)
               interval = Math.min (opTimeout, interval);

            // Shrink the interval to an eighth to get better accuracy
            // while being conservative so we don't send heartbeats
            // too late. Also, make sure we run checks at least once
            // every second to get the pend ops machinery working
            // properly
            
            interval = Math.min (interval / 8, Sec);
            
            resetInterval (interval, new TimerTask () {
                  public void run () { onTimer (); }
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
                      "req tsp: %s", nanoToStr (obj.getRequestTimestamp ()));
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
      if (log.isActiveAtLevel (Logger.Level.Trace))
         log.trace ("<= Sequence: next seq no: %d", obj.getNextSeqNo ());
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
      if (isThisSessionPacked (obj.getSessionIdPrefix ()))
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
         traceResponse (obj, obj.getSessionId (), obj.getRequestTimestamp (),
                        String.format (", next seq no: %d",
                                       obj.getNextSeqNo ()));

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
               log.trace ("<= Unsequenced app message: %s", getMsgType (o));
            dispatchMsg (o);
         }
      }
   }

   private void onPacketStart ()
   {
      nextActualIncomingSeqNo = 0;
   }
   
   private void onPacketEnd ()
   {
      nextActualIncomingSeqNo = 0;
   }
   
   private void onFailedToSendAppMsg (Object msg, Throwable e)
   {
      if (msg != null)
         innerTerminate ("Failed to send application message: " +
                         getMsgType (msg), e, TerminationCode.UnspecifiedError);
      else
         innerTerminate ("Failed to send an application message", e,
                         TerminationCode.UnspecifiedError);
   }
   
   private static class PendMsg
   {
      PendMsg (long seqNo, Object msg)
      {
         this.seqNo = seqNo;
         this.msg = msg;
      }

      long seqNo;
      Object msg;
   }

   public class OperationObs
   {
      public void onApplied (Applied msg)
      {
         long from = msg.getFrom ();
         int count = msg.getCount ();
         reapPendOps (from, count);
         obs.onOperationsApplied (from, count);
      }

      public void onNotApplied (NotApplied msg)
      {
         long from = msg.getFrom ();
         int count = msg.getCount ();
         reapPendOps (from, count);
         obs.onOperationsNotApplied (from, count);
      }
   }

   private boolean startFrame (long nextSeqNo, String what)
   {
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
               log.warn ("%s comes in too low: seqno %s < %s", what, nextSeqNo,
                         nextExpectedIncomingSeqNo);
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
            log.trace ("<= Sequenced app message: %s, seq no: %d",
                       getMsgType (msg), actualSn);
         
         if (actualSn == nextExpectedIncomingSeqNo)
         {
            ++ nextExpectedIncomingSeqNo;
            dispatchMsg (msg);

            if (firstSeqNoInQueue != 0 &&
                nextExpectedIncomingSeqNo >= firstSeqNoInQueue)
               flushPendIncoming ();
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
         {
            if (log.isActiveAtLevel (Logger.Level.Trace))
               log.trace ("Enqueued out of sequence message: %s",
                          getMsgType (msg));
            synchronized (pendIncoming)
            {
               if (pendIncoming.isEmpty ())
                  firstSeqNoInQueue = actualSn;
               pendIncoming.addLast (new PendMsg (actualSn, msg));
            }
         }
      }
      else
         log.warn ("Ignoring already seen message %s with seqno %s, " +
                   "next expected seqno is %s", getMsgType (msg), actualSn,
                      nextExpectedIncomingSeqNo);
   }

   private void onMissingSequence (Object msg)
   {
      String reason = "No sequencing message seen when receiving " +
         getMsgType (msg);
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

      log.info ("Inbound sequence gap detected: expected %s but got %s",
                nextExpectedIncomingSeqNo, to);

      requestRetransmit ();
   }
   
   private void requestRetransmit ()
   {
      long tsp = nowNano ();
      synchronized (pendRetReqs)
      {
         pendRetReqs.add (tsp);
      }

      int count = (int) (requestedRetransmitEnd - nextExpectedIncomingSeqNo);
      
      RetransmitRequest rr = new RetransmitRequest ();
      rr.setSessionId (sessionIdBytes);
      rr.setTimestamp (tsp);
      rr.setFromSeqNo (nextExpectedIncomingSeqNo);
      rr.setCount (count);
      
      log.info ("Requesting retransmission of %d messages " +
                "from seq no %d (req tsp: %s)", count,
                nextExpectedIncomingSeqNo, nanoToStr (tsp));
      
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

   private void flushPendIncoming ()
   {
      int dequeued = 0;

      synchronized (pendIncoming)
      {
         while (! pendIncoming.isEmpty ())
         {
            PendMsg pend = pendIncoming.peekFirst ();
            if (pend.seqNo == nextExpectedIncomingSeqNo)
            {
               ++ nextExpectedIncomingSeqNo;
               ++ dequeued;
               dispatchMsg (pend.msg);
               pendIncoming.removeFirst ();
            }
            else if (pend.seqNo < nextExpectedIncomingSeqNo)
               pendIncoming.removeFirst ();
            else
               break;
         }

         if (! pendIncoming.isEmpty ())
            firstSeqNoInQueue = pendIncoming.peekFirst ().seqNo;
         else
         {
            firstSeqNoInQueue = 0;
            flushPendTerm ();
         }
      }
      
      if (dequeued > 0 && log.isActiveAtLevel (Logger.Level.Trace))
         log.trace ("Dequeued %d messages after retransmit complete", dequeued);
   }

   private void flushPendTerm ()
   {
      if (pendTerm)
      {
         pendTerm = false;
         innerTerminate ("Peer finished sending", "finished receiving", null,
                         TerminationCode.Finished);            
      }
   }
   
   private void innerTerminate (String logReason, String sendReason,
                                Throwable cause, TerminationCode code)
   {
      log.trace ("=> Terminate: %s", logReason);
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

   private synchronized void innerSend (Object msg)
      throws IOException, BlinkException
   {
      sentMsg ();
      wr.write (msg);
      flush ();
   }
   
   private synchronized void innerSend (Iterable<?> msgs)
      throws IOException, BlinkException
   {
      sentMsg ();
      wr.write (msgs);
      flush ();
   }
   
   private synchronized void innerSend (Object [] msgs, int from, int len)
      throws IOException, BlinkException
   {
      sentMsg ();
      wr.write (msgs, from, len);
   }

   private synchronized long innerSendIdemp (Object msg)
      throws IOException, BlinkException
   {
      sentMsg ();
      long sn = nextOutgoingSeqNo ++;
      pushPendOps (sn, 1);
      wr.write (getSeqPrefix (sn));
      wr.write (msg);
      flush ();
      return sn;
   }
   
   private synchronized long innerSendIdemp (Iterable<?> msgs)
      throws IOException, BlinkException
   {
      sentMsg ();
      long firstSn = nextOutgoingSeqNo;
      int count = getIterableSize (msgs);
      nextOutgoingSeqNo += count;
      pushPendOps (firstSn, count);
      wr.write (getSeqPrefix (firstSn));
      wr.write (msgs);
      flush ();
      return firstSn;
   }
   
   private synchronized long innerSendIdemp (Object [] msgs, int from, int len)
      throws IOException, BlinkException
   {
      sentMsg ();
      long firstSn = nextOutgoingSeqNo;
      nextOutgoingSeqNo += len;
      pushPendOps (firstSn, len);
      wr.write (getSeqPrefix (firstSn));
      wr.write (msgs, from, len);
      flush ();
      return firstSn;
   }

   private void allocIdempSeqNo (long seqNo, int count)
   {
      long next = seqNo + count;
      if (next > nextOutgoingSeqNo)
         nextOutgoingSeqNo = next;
   }

   private synchronized void innerResendIdemp (Object msg, long seqNo)
      throws IOException, BlinkException
   {
      sentMsg ();
      allocIdempSeqNo (seqNo, 1);
      pushPendResendOps (seqNo, 1);
      wr.write (getSeqPrefix (seqNo));
      wr.write (msg);
      flush ();
   }

   private synchronized void innerResendIdemp (Iterable<?> msgs,
                                               long firstSeqNo)
      throws IOException, BlinkException
   {
      int count = getIterableSize (msgs);
      allocIdempSeqNo (firstSeqNo, count);
      pushPendResendOps (firstSeqNo, count);
      sentMsg ();
      wr.write (getSeqPrefix (firstSeqNo));
      wr.write (msgs);
      flush ();
   }
   
   private synchronized void innerResendIdemp (Object [] msgs, int from,
                                               int len, long firstSeqNo)
      throws IOException, BlinkException
   {
      sentMsg ();
      allocIdempSeqNo (firstSeqNo, len);
      pushPendResendOps (firstSeqNo, len);
      wr.write (getSeqPrefix (firstSeqNo));
      wr.write (msgs, from, len);
      flush ();
   }

   private void pushPendOps (long seqNo, int count)
   {
      long t = now ();
      long secs = t / 1000;
      long normalizedTsp = secs * Sec;
      long end = seqNo + count;
      PendOpRange range = pendOps [(int)(secs % pendOpsSize)];

      if (range.tsp != normalizedTsp) // Fresh or stale range
      {
         if (range.tsp != 0) // Stale
            reportStaleOps (range);
         range.from = seqNo;
         range.end = end;
         range.tsp = normalizedTsp;
      }
      else // Append to current range
      {
         assert seqNo == range.end;
         range.end += count;
      }
   }

   private void pushPendResendOps (long seqNo, int count)
   {
      pendResends.addLast (new PendOpRange (now (), seqNo, seqNo + count));
   }
   
   private synchronized void reapPendOps (long from, int count)
   {
      long to = from + count - 1;
      for (PendOpRange range : pendOps)
         reapPendOpRange (from, to, range);

      if (! pendResends.isEmpty ())
         for (PendOpRange range : pendResends)
            reapPendOpRange (from, to, range);
   }

   private boolean intersects (long from, long to, PendOpRange range)
   {
      return ! (to < range.from || from >= range.end);
   }
   
   private void reapPendOpRange (long from, long to, PendOpRange range)
   {
      if (range.tsp != 0 && intersects (from, to, range))
      {
         if (from > range.from)
            log.warn ("Missing ack for ops: %d ... %d", range.from, from - 1);
         range.from = Math.min (to + 1, range.end);
         if (range.from == range.end)
            range.tsp = 0;
      }
   }

   private void checkPendOpsTimeout (long now)
   {
      long secs = now / 1000;
      PendOpRange range = pendOps [(int)((secs + 1) % pendOpsSize)];
      if (range.tsp != 0)
         reportStaleOps (range);

      while (! pendResends.isEmpty ())
      {
         PendOpRange head = pendResends.peekFirst ();
         long elapsed = now - head.tsp;
         if (elapsed >= opTimeout)
         {
            if (head.from < head.end)
               reportStaleOps (head);
            pendResends.removeFirst ();
         }
         else
            break;
      }
   }

   private void reportStaleOps (PendOpRange range)
   {
      obs.onOperationsTimeout (range.from, (int)(range.end - range.from));
      range.tsp = 0;
   }
   
   private Object getSeqPrefix (long sn)
   {
      seqPrefix.setNextSeqNo (sn);
      return seqPrefix;
   }
   
   private void flush () throws IOException
   {
      outBuf.flip ();
      outBb.limit (outBuf.size ());
      outBb.position (0);
      ch.write (outBb);
      outBuf.clear ();
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
               log.warn ("Unknown %s tsp: %s", what, nanoToStr (reqTsp));
         }
      }
      else
         log.warn ("%s session id mismatch: expected: %s != actual: %s",
                   what, sessionId, toUuid (snId));
      
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

   private boolean isThisSessionPacked (byte [] id)
   {
      return java.util.Arrays.equals (id, packedSessionIdBytes);
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

   private final static int NegotiateTimeout = 2 * Sec;

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
            log.trace ("=> Negotiate (req tsp: %s)", nanoToStr (tsp));

         Negotiate n = new Negotiate ();

         n.setSessionId (sessionIdBytes);
         n.setTimestamp (tsp);
         if (isIdempotent)
            n.setClientFlow (FlowType.Idempotent);
         else
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

   private final static int EstablishmentTimeout = 2 * Sec;
   
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
            log.trace ("=> Establish (req tsp: %s)", nanoToStr (tsp));

         Establish m = new Establish ();

         m.setSessionId (sessionIdBytes);
         m.setTimestamp (tsp);
         m.setKeepaliveInterval (keepaliveInterval);
         m.setCredentials (credentials);

         innerSend (m);

         resetTimer (EstablishmentTimeout, new TimerTask () {
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
         log.warn (getMsgType (msg) + " received before establish");
         return false;
      }
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
         log.warn ("No establishment response, retrying");
         establish ();
      }
   }

   private void checkServerIsAlive (long now)
   {
      long elapsed = now - lastMsgReceivedTsp;
      if (elapsed > 3 * serverKeepaliveInterval)
         innerTerminate ("Session timed out after receiving no message in " +
                         elapsed + "ms", null, TerminationCode.Timeout);
   }

   private void showClientIsAlive (long now)
   {
      long elapsed = now - lastMsgSentTsp;
      if (elapsed + timerInterval >= (long)(keepaliveInterval * 0.9))
         try
         {
            if (isIdempotent)
            {
               log.trace ("=> Sequence (Heartbeat)");
               innerSend (getSeqPrefix (nextOutgoingSeqNo));
            }
            else
            {
               log.trace ("=> UnsequencedHeartbeat");
               innerSend (new UnsequencedHeartbeat ());
            }
         }
         catch (Exception e)
         {
            innerTerminate ("Failed to send unsequenced heartbeat", e,
                            TerminationCode.UnspecifiedError);
         }
   }

   private void onTimer ()
   {
      long t = now ();
      checkPendOpsTimeout (t);
      showClientIsAlive (t);
      checkServerIsAlive (t);
   }

   private void traceResponse (Object msg, byte [] snId, long tsp, String extra)
   {
      log.trace ("<= %s (id: %s, req tsp: %s%s)",
                 getMsgType (msg), toUuid (snId), nanoToStr (tsp), extra);
   }

   private void traceResponse (Object msg, byte [] snId, long tsp)
   {
      traceResponse (msg, snId, tsp, "");
   }

   private static String getMsgType (Object o)
   {
      return o.getClass ().getName ();
   }

   private final Client.FlowType flowType;
   private final EventObserver obs;
   private final DefaultObsRegistry appRegistry;
   private final Dispatcher appDispatcher;
   private final ArrayDeque<PendMsg> pendIncoming;
   private final Sequence seqPrefix;
   private final boolean isIdempotent;
   private final UUID sessionId;
   private final byte[] sessionIdBytes;
   private final byte[] packedSessionIdBytes;
   private final HashSet<Long> pendNegReqs;
   private final HashSet<Long> pendEstReqs;
   private final HashSet<Long> pendRetReqs;
   private final int pendOpsSize;
   private final PendOpRange [] pendOps;
   private final ArrayDeque<PendOpRange> pendResends;

   private final DatagramChannel ch;
   private final CompactWriter wr;
   private final CompactReader rd;
   private final ByteBuffer inBb;
   private final ByteBuf inBuf;
   private final ByteBuffer outBb;
   private final ByteBuf outBuf;
   private final int opTimeout;
   private final int keepaliveInterval;

   private volatile boolean done;
   private Object credentials;
   
   private volatile int serverKeepaliveInterval;
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
   private static Timer timer = new Timer ("XmitClientSessionTimer",
                                           true /* daemon */);
   private boolean isSeqSrv;
   private boolean pendTerm;
   private long nextOutgoingSeqNo;
   private volatile long lastAckedOutgoingSeqNo;
   private boolean isRetransmit;

   private final Logger log =
      Logger.Manager.getLogger (com.pantor.xmit.Client.Session.class);
}
