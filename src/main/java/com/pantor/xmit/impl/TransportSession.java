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

import java.nio.channels.DatagramChannel;

import com.pantor.blink.ObjectModel;
import com.pantor.blink.Logger;
import com.pantor.blink.DefaultObsRegistry;
import com.pantor.blink.CompactReader;
import com.pantor.blink.CompactWriter;
import com.pantor.blink.ByteBuf;
import com.pantor.blink.Dispatcher;
import com.pantor.blink.Observer;
import com.pantor.blink.BlinkException;

import java.nio.channels.DatagramChannel;
import java.nio.channels.Selector;
import java.nio.channels.SelectionKey;
import java.nio.ByteBuffer;

import java.io.IOException;
import java.net.SocketAddress;
import java.net.StandardSocketOptions;
import java.net.InetSocketAddress;
import java.util.Set;
import java.util.Iterator;

import static com.pantor.xmit.impl.Util.*;

public final class TransportSession implements Runnable
{
   private final static int Sec = 1000;
   private final static int TransportSessionEstablishmentTimeout = 5 * Sec;
   
   TransportSession (DatagramChannel ch, ByteBuffer bb, Server srv)
      throws BlinkException
   {
      this.ch = ch;
      this.srv = srv;
      this.inBb = bb;
      this.inBuf = new ByteBuf (inBb.array (), 0, inBb.limit ());
      DefaultObsRegistry oreg = new DefaultObsRegistry (srv.getObjectModel ());
      oreg.addObserver (this);
      this.rd = new CompactReader (srv.getObjectModel (), oreg);
      this.outBb = ByteBuffer.allocate (1500);
      this.outBuf = new ByteBuf (outBb.array ());
      this.wr = new CompactWriter (srv.getObjectModel (), outBuf);
      this.timeout = TransportSessionEstablishmentTimeout;
   }

   @Override
   public void run ()
   {
      try
      {
         // Process initial packet

         decodePacket ();
            
         // Enter event loop

         eventLoop ();
      }
      catch (Throwable e)
      {
         log.error (e, "%s: %s", info (ch), getInnerCause (e));;
      }
   }

   void updateTimeout (int timeout)
   {
      this.timeout = Math.min (this.timeout, timeout);
   }
      
   private void eventLoop () throws IOException
   {
      try
      {
         long effectiveTimeout = timeout / 8;
         Selector selector = Selector.open ();
         SelectionKey key = ch.register (selector, SelectionKey.OP_READ);
         while (! done)
         {
            int n = selector.select (effectiveTimeout);
            if (n > 0)
               try
               {
                  Set<SelectionKey> keys = selector.selectedKeys ();
                  Iterator<SelectionKey> i = keys.iterator();
                  while (i.hasNext ())
                  {
                     SelectionKey k = i.next ();
                     if (k.isReadable ())
                        receivePacket ();
                     i.remove();
                  }
               }
               catch (java.net.PortUnreachableException e)
               {
                  log.warn ("%s: Port is unreachable, terminating transport " +
                            "session", info (ch));
                  ServerSession s = xmitSession;
                  if (s != null)
                     s.onTransportLost ("Port unreachable", e);
                  done = true;
               }
               catch (Throwable e)
               {
                  log.warn (e, "%s: Terminating transport session: %s",
                            info (ch), getInnerCause (e));
                  ServerSession s = xmitSession;
                  if (s != null)
                     s.onTransportLost (getInnerCause (e), e);
                  done = true;
               }
            else
               handleTimeout ();

            ServerSession s = xmitSession;
            if (s != null)
               s.checkTimers (effectiveTimeout);
         }
      }
      finally
      {
         ch.close ();
         done = true;
      }
   }

   private void handleTimeout ()
   {
      if (xmitSession == null)
      {
         if (pendTerm || negRejected)
         {
            log.info ("%s: Transport session terminated", info (ch));
            done = true;
         }
         else
         {
            log.warn ("%s: No Xmit:Establish request received " +
                      "within the last %s seconds, giving up on" +
                      " this transport", info (ch),
                      TransportSessionEstablishmentTimeout / 1000);
         }
         done = true;
      }
   }
      
   private void receivePacket () throws BlinkException, IOException
   {
      inBb.clear ();
      ch.receive (inBb);
      inBb.flip ();
      try
      {
         if (inBb.limit () > 0)
         {
            inBuf.clear ();
            inBuf.setPos (inBb.limit ());
            inBuf.flip ();
            decodePacket ();
         }
         else
         {
            log.warn ("%s: Empty packet", info (ch));
         }
      }
      finally
      {
         rd.reset ();
      }
   }

   private void decodePacket () throws BlinkException, IOException
   {
      ServerSession s = xmitSession;
      if (s != null)
         s.onPacketStart ();
      rd.read (inBuf);
      if (! rd.isComplete ())
      {
         rd.reset ();
         log.warn ("%s: Incomplete Blink content in packet", info (ch));
      }
      if (s != null)
         s.onPacketEnd ();
   }

   private boolean checkEstablished (Object o)
   {
      if (xmitSession != null)
         return true;
      else
      {
         log.warn ("%s: Ignoring unsolicited message on " +
                   "not established session: %s", info (ch), getMsgType (o));
         return false;
      }
   }

   public void onNegotiate (xmit.Negotiate neg)
   {
      if (log.isActiveAtLevel (Logger.Level.Trace))
         traceRequest (neg, neg.getSessionId (), neg.getTimestamp ());

      srv.negotiate (neg, this);
   }

   public void onEstablish (xmit.Establish est)
   {
      if (log.isActiveAtLevel (Logger.Level.Trace))
         traceRequest (est, est.getSessionId (), est.getTimestamp ());
      
      if (xmitSession == null)
         xmitSession = srv.establish (est, this);
      else
         sendEstRej (est, xmit.EstablishmentRejectCode.Unspecified,
                     "Multiplexing not supported");
   }
      
   public void onAny (Object o)
   {
      if (log.isActiveAtLevel (Logger.Level.Trace))
         log.trace ("=> App message: %s", getMsgType (o));
            
      if (checkEstablished (o))
      {
         ServerSession s = xmitSession;
         if (s != null)
            s.onApplicationMsg (o);
      }
   }

   public void onContext (xmit.Context o)
   {
      if (checkEstablished (o))
      {
         ServerSession s = xmitSession;
         if (s != null)
            s.onContext (o);
      }
   }

   public void onPackedContext (xmit.PackedContext o)
   {
      if (checkEstablished (o))
      {
         ServerSession s = xmitSession;
         if (s != null)
            s.onPackedContext (o);
      }
   }
      
   public void onRetransmitRequest (xmit.RetransmitRequest o)
   {
      if (checkEstablished (o))
      {
         ServerSession s = xmitSession;
         if (s != null)
            s.onRetransmitRequest (o);
      }
   }
      
   public void onTerminate (xmit.Terminate o)
   {
      if (checkEstablished (o))
      {
         ServerSession s = xmitSession;
         if (s != null)
            s.onTerminate (o);
      }
   }
      
   public void onSequence (xmit.Sequence o)
   {
      if (checkEstablished (o))
      {
         ServerSession s = xmitSession;
         if (s != null)
            s.onSequence (o);
      }
   }
      
   public void onFinishedSending (xmit.FinishedSending o)
   {
      if (checkEstablished (o))
      {
         ServerSession s = xmitSession;
         if (s != null)
            s.onFinishedSending (o);
      }
   }
      
   // FIXME: Recognize (and ignore/warn) all messages in xmit namespace
      
/* FIXME
   @Blink.Ns ("Xmit")
   public void onAnyOtherXmit (Object o)
   {
   log.warn ("%s: Ignoring Xmit message: %s", info (ch),
   getMsgType (o));
   }
*/

   void sendEstRej (xmit.Establish est, xmit.EstablishmentRejectCode code,
                    String reason)
   {
      log.warn ("%s [%s]: Establish rejected: %s", info (ch),
                toUuid (est.getSessionId ()), reason);
      xmit.EstablishmentReject rej = new xmit.EstablishmentReject ();
      rej.setRequestTimestamp (est.getTimestamp ());
      rej.setSessionId (est.getSessionId ());
      rej.setCode (code);
      rej.setReason (reason);

      if (log.isActiveAtLevel (Logger.Level.Trace))
         traceResponse (rej, rej.getSessionId (), rej.getRequestTimestamp ());

      sendSafe (rej);
   }

   void sendNegRej (xmit.Negotiate est, xmit.NegotiationRejectCode code,
                    String reason)
   {
      log.warn ("%s [%s]: Negotiate rejected: %s", info (ch),
                toUuid (est.getSessionId ()), reason);
      xmit.NegotiationReject rej = new xmit.NegotiationReject ();
      rej.setRequestTimestamp (est.getTimestamp ());
      rej.setSessionId (est.getSessionId ());
      rej.setCode (code);
      rej.setReason (reason);

      if (log.isActiveAtLevel (Logger.Level.Trace))
         traceResponse (rej, rej.getSessionId (), rej.getRequestTimestamp ());

      sendSafe (rej);
      negRejected = true;
   }

   public void sendSafe (Object msg)
   {
      try
      {
         send (msg);
      }
      catch (Exception e)
      {
         String reason = String.format (
            "%s: Cannot send %s: %s", info (ch), getMsgType (msg),
            getInnerCause (e));
         ServerSession s = xmitSession;
         if (s != null)
            s.innerTerminate (reason, e, xmit.TerminationCode.UnspecifiedError);
         else
            log.warn (reason);
      }
   }
      
   public synchronized void send (Object msg)
      throws BlinkException, IOException
   {
      wr.write (msg);
      flush ();
   }

   public synchronized void send (Object msg1, Object msg2)
      throws BlinkException, IOException
   {
      wr.write (msg1);
      wr.write (msg2);
      flush ();
   }

   public synchronized void send (Object [] msgs)
      throws BlinkException, IOException
   {
      wr.write (msgs);
      flush ();
   }

   public synchronized void send (Object msg, Object [] msgs)
      throws BlinkException, IOException
   {
      wr.write (msg);
      wr.write (msgs);
      flush ();
   }

   public synchronized void send (Object [] msgs, int from, int len)
      throws BlinkException, IOException
   {
      wr.write (msgs, from, len);
      flush ();
   }

   public synchronized void send (Object msg, Object [] msgs, int from,
                                  int len)
      throws BlinkException, IOException
   {
      wr.write (msg);
      wr.write (msgs, from, len);
      flush ();
   }

   public synchronized void send (Iterable<?> msgs)
      throws BlinkException, IOException
   {
      wr.write (msgs);
      flush ();
   }

   public synchronized void send (Object msg, Iterable<?> msgs)
      throws BlinkException, IOException
   {
      wr.write (msg);
      wr.write (msgs);
      flush ();
   }

   private void flush () throws IOException
   {
      outBuf.flip ();
      outBb.limit (outBuf.size ());
      outBb.position (0);
      ch.write (outBb);
      outBuf.clear ();
   }
      
   public synchronized void terminate (String reason, byte [] sessionId,
                                       xmit.TerminationCode code)
   {
      pendTerm = true;
      xmitSession = null;
      timeout = 1; // Don't linger
      xmit.Terminate t = new xmit.Terminate ();
      t.setSessionId (sessionId);
      t.setCode (code);
      if (reason != null && ! reason.isEmpty ())
         t.setReason (reason);

      try
      {
         send (t);
      }
      catch (Exception e)
      {
         log.warn ("%s: Graceful termination failed: %s", info (ch),
                   getInnerCause (e));
      }
   }

   SocketAddress getSourceAddress () throws IOException
   {
      return ch.getRemoteAddress ();
   }

   void traceResponse (Object msg, byte [] snId, long tsp)
   {
      log.trace ("<= %s (id: %s, req tsp: %s)", getMsgType (msg),
                 toUuid (snId), nanoToStr (tsp));
   }

   void traceRequest (Object msg, byte [] snId, long tsp)
   {
      log.trace ("=> %s (id: %s, req tsp: %s)", getMsgType (msg),
                 toUuid (snId), nanoToStr (tsp));
   }

   private final DatagramChannel ch;
   private final CompactWriter wr;
   private final CompactReader rd;
   private final ByteBuffer inBb;
   private final ByteBuf inBuf;
   private final ByteBuffer outBb;
   private final ByteBuf outBuf;
   private final Server srv;
   private final Logger log =
      Logger.Manager.getLogger (TransportSession.class);
   private int timeout;
   private volatile ServerSession xmitSession;
   private volatile boolean done;
   private volatile boolean pendTerm;
   private volatile boolean negRejected;
}
