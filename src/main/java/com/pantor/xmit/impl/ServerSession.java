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

import com.pantor.xmit.XmitException;

import java.io.IOException;
import java.util.UUID;

import static com.pantor.xmit.impl.Util.*;

import com.pantor.blink.NsName;
import com.pantor.blink.ObjectModel;
import com.pantor.blink.Logger;
import com.pantor.blink.DefaultObsRegistry;
import com.pantor.blink.Dispatcher;
import com.pantor.blink.Observer;
import com.pantor.blink.BlinkException;
import java.util.Arrays;

import com.pantor.xmit.Server.Journal;
import com.pantor.xmit.Server.Session.EventObserver;

final class ServerSession implements com.pantor.xmit.Server.Session
{
   private final static int PackedPrefixLen = 8;
   
   public ServerSession (UUID sessionId,
                         int keepaliveInterval, ObjectModel om)
   {
      this.useAutoApply = true;
      this.keepaliveInterval = keepaliveInterval;
      this.nextActualIncomingSeqNo = 0;
      this.nextExpectedIncomingSeqNo = 1;
      this.nextOutgoingSeqNo = 1;
      this.sessionId = sessionId;
      this.appRegistry = new DefaultObsRegistry (om);
      this.appDispatcher = new Dispatcher (om, appRegistry);
      this.sessionIdBytes = toBytes (sessionId);
      this.packedSessionIdBytes =
         Arrays.copyOfRange (sessionIdBytes, 0, PackedPrefixLen);
      this.seqPrefix = new xmit.Sequence ();
      this.retransPrefix = new xmit.Retransmission ();
      this.appliedMsg = new xmit.Applied ();
      this.notAppliedMsg = new xmit.NotApplied ();
      appliedMsg.setCount (1);
      retransPrefix.setSessionId (sessionIdBytes);
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
   public void addAppObserver (Object obs, String prefix)
      throws XmitException
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
   public void terminate (String reason)
   {
      innerTerminate (reason, reason, null, xmit.TerminationCode.Finished);
   }

   @Override
   public void finish ()
   {
      // FIXME: Send FinishedSending and terminate when
      // FinishedReceiving is returned or after timeout
   }

   private boolean isEstablished ()
   {
      return establishedTsport != null;
   }
      
   synchronized boolean establish (xmit.Establish est, TransportSession tsport)
   {
      if (! isEstablished ())
      {
         EstReqImpl req = new EstReqImpl (est, tsport);
         if (eventObs != null)
            eventObs.onEstablish (req, this);
         if (! req.wasRejected ())
         {
            // FIXME: Enable when/if implemented
            // if (obj.hasNextSeqNo ())
            //    startFrame (obj.getNextSeqNo ());
            lastPacketReceivedTsp = now ();
            establishedTsport = tsport;
            clientKeepaliveInterval = est.getKeepaliveInterval ();
            tsport.updateTimeout (clientKeepaliveInterval);
            xmit.EstablishmentAck ack = new xmit.EstablishmentAck ();
            ack.setSessionId (est.getSessionId ());
            ack.setRequestTimestamp (est.getTimestamp ());
            ack.setKeepaliveInterval (keepaliveInterval);
            ack.setNextSeqNo (nextOutgoingSeqNo);

            if (log.isActiveAtLevel (Logger.Level.Trace))
               tsport.traceResponse (ack, ack.getSessionId (),
                                     ack.getRequestTimestamp ());

            sentMsg ();
            tsport.sendSafe (ack);
            return true;
         }
      }
      else
         tsport.sendEstRej (
            est, xmit.EstablishmentRejectCode.AlreadyEstablished,
            "Already established");
         
      return false;
   }

   void onUnsequencedHeartbeat (xmit.UnsequencedHeartbeat obj)
   {
      log.trace ("=> UnsequencedHeartbeat");
   }

   void onContext (xmit.Context obj)
   {
      if (isThisSession (obj.getSessionId ()))
      {
         if (obj.hasNextSeqNo ())
            nextActualIncomingSeqNo = obj.getNextSeqNo ();
      }
      else
         innerTerminate ("Xmit:Context: Session multiplexing not supported",
                         null, xmit.TerminationCode.UnspecifiedError);
   }

   void onPackedContext (xmit.PackedContext obj)
   {
      if (isThisSessionPacked (obj.getSessionIdPrefix ()))
      {
         if (obj.hasNextSeqNo ())
            nextActualIncomingSeqNo = obj.getNextSeqNo ();
      }
      else
         innerTerminate ("Xmit:PackedContext: Session multiplexing not " +
                         "supported", null,
                         xmit.TerminationCode.UnspecifiedError);
   }

   private boolean retransIsWithinBounds (long from, int count)
   {
      return from > 0 && from < nextOutgoingSeqNo &&
         from + count <= nextOutgoingSeqNo;
   }
   
   void onRetransmitRequest (xmit.RetransmitRequest req)
   {
      log.trace ("=> RetransmitRequest");
      if (isThisSession (req.getSessionId ()))
      {
         long reqTsp = req.getTimestamp ();
         long from = req.getFromSeqNo ();
         int count = req.getCount ();
         if (retransIsWithinBounds (from, count))
         {
            try
            {
               // FIXME: Bundle multiple messages per retransmit request
               log.info ("Retransmitting one message (seq no: %d, req tsp: %s)",
                         from, nanoToStr (reqTsp));
               Object [] msgs = new Object [1];
               journal.read (from, 1, msgs);
               retransmit (msgs [0], from, reqTsp);
            }
            catch (Throwable e)
            {
               innerTerminate ("Retransmit failed", e,
                               xmit.TerminationCode.UnspecifiedError);
            }
         }
         else
         {
            innerTerminate ("Retransmit out of bounds", null,
                            xmit.TerminationCode.ReRequestOutOfBounds);
         }
      }
   }

   private synchronized void retransmit (Object msg, long sn, long reqTsp)
      throws IOException, BlinkException, XmitException
   {
      sentMsg ();
      retransPrefix.setRequestTimestamp (reqTsp);
      retransPrefix.setNextSeqNo (sn);
      retransPrefix.setCount (1);
      establishedTsport.send (retransPrefix, msg);
   }

   void onSequence (xmit.Sequence obj)
   {
      if (log.isActiveAtLevel (Logger.Level.Trace))
         log.trace ("=> Sequence: NextSeqNo: %d", obj.getNextSeqNo ());
      startFrame (obj.getNextSeqNo ());
   }

   void onTerminate (xmit.Terminate obj)
   {
      log.trace ("=> Terminate");
      if (isThisSession (obj.getSessionId ()))
      {
         String logReason;
         if (obj.hasReason () && ! obj.getReason ().isEmpty ())
            logReason = obj.getReason ();
         else
            logReason = "Terminated by peer: " + obj.getCode ();

         innerTerminate (logReason, "goodbye", null,
                         xmit.TerminationCode.Finished);
      }
   }

   void onFinishedSending (xmit.FinishedSending obj)
   {
      log.trace ("=> FinishedSending");
      if (isThisSession (obj.getSessionId ()))
      {
         pendTermAfterFinished = true;
         if (startFrame (obj.getLastSeqNo () + 1)) // FIXME: applied?
            flushPendTermAfterFinished ();
      }
   }

   void onApplicationMsg (Object o)
   {
      if (nextActualIncomingSeqNo >= nextExpectedIncomingSeqNo)
      {
         if (nextActualIncomingSeqNo > nextExpectedIncomingSeqNo)
            sendNotApplied ();
         nextExpectedIncomingSeqNo = ++ nextActualIncomingSeqNo;
         dispatchMsg (o);
         if (useAutoApply)
            sendApplied ();
      }
      else
      {
         if (log.isActiveAtLevel (Logger.Level.Trace))
            log.trace ("=> Ignoring duplicate app message: %s, seq no %d",
                       getMsgType (o), nextActualIncomingSeqNo);
      }
   }

   private void sendNotApplied ()
   {
      int count = (int)(nextActualIncomingSeqNo - nextExpectedIncomingSeqNo);
      notAppliedMsg.setFrom (nextExpectedIncomingSeqNo);
      notAppliedMsg.setCount (count);
      try
      {
         if (log.isActiveAtLevel (Logger.Level.Trace))
            log.trace ("<= Sending Xmit:NotApplied, from: %d, count: %d",
                       nextExpectedIncomingSeqNo, count);
         
         sendSequenced (notAppliedMsg);
      }
      catch (Throwable e)
      {
         innerTerminate ("Failed to send Xmit:NotApplied message", e,
                         xmit.TerminationCode.UnspecifiedError);
      }
   }

   private void sendApplied ()
   {
      long sn = nextExpectedIncomingSeqNo - 1;
      appliedMsg.setFrom (sn);

      try
      {
         if (log.isActiveAtLevel (Logger.Level.Trace))
            log.trace ("<= Sending Xmit:Applied, from: %d, count: %d", sn, 1);
         
         sendSequenced (appliedMsg, sn);
      }
      catch (Throwable e)
      {
         innerTerminate ("Failed to send Xmit:Applied message", e,
                         xmit.TerminationCode.UnspecifiedError);
      }
   }

   void onPacketStart ()
   {
      lastPacketReceivedTsp = now ();
      nextActualIncomingSeqNo = 0;
      if (eventObs != null)
         eventObs.onDatagramStart (this);
   }
   
   void onPacketEnd ()
   {
      if (eventObs != null)
         eventObs.onDatagramEnd (this);
      nextActualIncomingSeqNo = 0;
   }

   private void onFailedToSendAppMsg (Object msg, Throwable e)
   {
      if (msg != null)
         innerTerminate ("Failed to send application message: " +
                         msg.getClass ().getName (), e,
                         xmit.TerminationCode.UnspecifiedError);
      else
         innerTerminate ("Failed to send an application message", e,
                         xmit.TerminationCode.UnspecifiedError);
   }
   
   private boolean startFrame (long nextSeqNo)
   {
      nextActualIncomingSeqNo = nextSeqNo;
      return nextSeqNo >= nextExpectedIncomingSeqNo;
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

   private void flushPendTermAfterFinished ()
   {
      if (pendTermAfterFinished)
      {
         // FIXME: Send finished receiving
         pendTermAfterFinished = false;
         innerTerminate ("peer finished sending", "finished receiving", null,
                         xmit.TerminationCode.Finished);            
      }
   }

   void innerTerminate (String logReason, String sendReason, Throwable cause,
      xmit.TerminationCode code)
   {
      log.trace ("<= Terminate: " + logReason);

      TransportSession tsport = establishedTsport;
      synchronized (this) { establishedTsport = null; }
      
      if (tsport != null)
      {
         try
         {
            tsport.terminate (sendReason, sessionIdBytes, code);
         }
         finally
         {
            if (eventObs != null)
               eventObs.onTerminate (
                  logReason != null ? logReason : "terminated", cause, this);
         }
      }
      else
         log.trace ("Terminating not established session");
   }

   void innerTerminate (String reason, Throwable cause,
                        xmit.TerminationCode code)
   {
      innerTerminate (reason, reason, cause, code);
   }

   void setJournal (Journal journal) 
   {
      try
      {
         assert sessionId.equals (journal.getSessionId ());
         assert journal.getLastIncomingSeqNo () ==
            nextExpectedIncomingSeqNo - 1;
         assert journal.getLastOutgoingSeqNo () == nextOutgoingSeqNo - 1;
      }
      catch (Throwable e)
      {
         assert false;
      }
         
      this.journal = journal;
   }
      
   void resume (Journal journal)
      throws IOException
   {
      assert sessionId.equals (journal.getSessionId ());
      this.journal = journal;
      nextExpectedIncomingSeqNo = journal.getLastIncomingSeqNo () + 1;
      nextOutgoingSeqNo = journal.getLastOutgoingSeqNo () + 1;
   }
      
   @Override
   public void send (Object msg) throws IOException, XmitException
   {
      if (log.isActiveAtLevel (Logger.Level.Trace))
         log.trace ("<= Sending app message %s", getMsgType (msg));

      try
      {
         sendSequenced (msg);
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
   public void send (Object [] msgs) throws IOException, XmitException
   {
      send (msgs, 0, msgs.length);
   }

   @Override
   public void send (Object [] msgs, int from, int len)
      throws IOException, XmitException
   {
      if (log.isActiveAtLevel (Logger.Level.Trace))
         log.trace ("<= Sending %s app messages", msgs.length);

      try
      {
         sendSequenced (msgs, from, len);
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
   public void send (Iterable<?> msgs) throws IOException, XmitException
   {
      if (log.isActiveAtLevel (Logger.Level.Trace))
         log.trace ("<= Sending app messages");

      try
      {
         sendSequenced (msgs);
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
   public void applied ()
   {
      sendApplied ();
   }
      
   @Override
   public void setUseAutoApply (boolean useAutoApply)
   {
      this.useAutoApply = useAutoApply;
   }
      
   @Override
   public void setEventObserver (EventObserver eventObs)
   {
      this.eventObs = eventObs;
   }
      
   private synchronized void innerSend (Object msg)
      throws IOException, BlinkException, XmitException
   {
      if (isEstablished ())
      {
         sentMsg ();
         establishedTsport.send (msg);
      }
      else
         throw new XmitException ("Session not established");
   }

   private synchronized void sendSequenced (Object msg)
      throws IOException, BlinkException, XmitException
   {
      if (isEstablished ())
      {
         sentMsg ();
         long sn = nextOutgoingSeqNo;
         journal.write (sn, msg);
         ++ nextOutgoingSeqNo;
         establishedTsport.send (getSeqPrefix (sn), msg);
      }
      else
         throw new XmitException ("Session not established");
   }

   private synchronized void sendSequenced (Object msg, long inSeqNo)
      throws IOException, BlinkException, XmitException
   {
      if (isEstablished ())
      {
         sentMsg ();
         long outSeqNo = nextOutgoingSeqNo;
         journal.write (outSeqNo, inSeqNo, msg);
         ++ nextOutgoingSeqNo;
         establishedTsport.send (getSeqPrefix (outSeqNo), msg);
      }
      else
         throw new XmitException ("Session not established");
   }
   
   private synchronized void sendSequenced (Object [] msgs, int from, int len)
      throws IOException, BlinkException, XmitException
   {
      if (isEstablished ())
      {
         sentMsg ();
         long firstSn = nextOutgoingSeqNo;
         journal.write (firstSn, msgs, from, len);
         nextOutgoingSeqNo += len;
         establishedTsport.send (getSeqPrefix (firstSn), msgs, from, len);
      }
      else
         throw new XmitException ("Session not established");
   }

   private synchronized void sendSequenced (Iterable<?> msgs)
      throws IOException, BlinkException, XmitException
   {
      if (isEstablished ())
      {
         sentMsg ();
         long firstSn = nextOutgoingSeqNo;
         journal.write (firstSn, msgs);
         nextOutgoingSeqNo += getIterableSize (msgs);
         establishedTsport.send (getSeqPrefix (firstSn), msgs);
      }
      else
         throw new XmitException ("Session not established");
   }

   private Object getSeqPrefix (long sn)
   {
      seqPrefix.setNextSeqNo (sn);
      return seqPrefix;
   }
   
   private void sentMsg ()
   {
      lastMsgSentTsp = now ();
   }
      
   private boolean isThisSession (byte [] id)
   {
      return java.util.Arrays.equals (id, sessionIdBytes);
   }

   private boolean isThisSessionPacked (byte [] id)
   {
      return java.util.Arrays.equals (id, packedSessionIdBytes);
   }
   
   public void checkTimers (long timeout)
   {
      checkClientIsAlive ();
      showServerIsAlive (timeout);
   }
      
   public void onTransportLost (String reason, Throwable cause)
   {
      innerTerminate (reason, cause, xmit.TerminationCode.UnspecifiedError);
   }

   private void checkClientIsAlive ()
   {
      long elapsed = now () - lastPacketReceivedTsp;
      if (elapsed > 3 * clientKeepaliveInterval)
         innerTerminate ("Session timed out after receiving no " +
                         "message in " +
                         elapsed + "ms", null, xmit.TerminationCode.Timeout);
   }

   private void showServerIsAlive (long timeout)
   {
      long elapsed = now () - lastMsgSentTsp;
      if (elapsed + timeout >= (long)(keepaliveInterval * 0.9))
         try
         {
            log.trace ("<= Sequence (Heartbeat): seq no: %d",
                       nextOutgoingSeqNo);
            xmit.Sequence hbt = new xmit.Sequence ();
            hbt.setNextSeqNo (nextOutgoingSeqNo);
            innerSend (hbt);
         }
         catch (Exception e)
         {
            innerTerminate ("Failed to send unsequenced heartbeat", e,
                            xmit.TerminationCode.UnspecifiedError);
         }
   }

   int getKeepaliveInterval ()
   {
      return keepaliveInterval;
   }
   
   private EventObserver eventObs;
   private final DefaultObsRegistry appRegistry;
   private final Dispatcher appDispatcher;
   private final UUID sessionId;
   private final byte[] sessionIdBytes;
   private final byte[] packedSessionIdBytes;
   private final xmit.Sequence seqPrefix;
   private final xmit.Retransmission retransPrefix;
   private final xmit.Applied appliedMsg;
   private final xmit.NotApplied notAppliedMsg;
   private final int keepaliveInterval;

   private Journal journal;
   private volatile int clientKeepaliveInterval;
   private volatile long nextExpectedIncomingSeqNo;
   private volatile long nextToApply;
   private volatile long nextActualIncomingSeqNo;
   private volatile long nextOutgoingSeqNo;
   private volatile long lastPacketReceivedTsp;
   private volatile long lastMsgSentTsp;

   private boolean useAutoApply;
      
   private boolean pendTermAfterFinished;
   volatile private TransportSession establishedTsport;

   private final Logger log = Logger.Manager.getLogger (
      com.pantor.xmit.Server.Session.class);
}

