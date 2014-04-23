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

import com.pantor.blink.ObjectModel;
import com.pantor.blink.Logger;
import com.pantor.blink.BlinkException;

import java.io.IOException;
import java.net.SocketAddress;
import java.net.StandardSocketOptions;
import java.net.InetSocketAddress;

import java.nio.channels.DatagramChannel;
import java.nio.ByteBuffer;

import java.util.HashMap;
import java.util.UUID;

import static com.pantor.xmit.impl.Util.*;

public class Server extends com.pantor.xmit.Server
{
   public Server (int port, ObjectModel om, NegotiationObserver negObs)
   {
      this.om = om;
      this.negObs = negObs;
      this.port = port;
      this.keepaliveInterval = DefaultKeepaliveInterval;
   }

   public Server (DatagramChannel ch, ObjectModel om,
                  NegotiationObserver negObs)
   {
      this.om = om;
      this.negObs = negObs;
      this.port = 0;
      this.ch = ch;
   }

   @Override
   public void setKeepaliveInterval (int keepaliveInterval)
   {
      this.keepaliveInterval = keepaliveInterval;
   }

   private final static int Sec = 1000;
   private final static int DefaultKeepaliveInterval = 2 * Sec;

   @Override
   public void mainLoop () throws XmitException, IOException
   {
      if (ch == null)
      {
         if (port > 0)
         {
            ch =
               DatagramChannel.open ()
               .setOption (StandardSocketOptions.SO_REUSEADDR, true);
            
            ch.bind (new InetSocketAddress (port));
         }
         else
            throw new IllegalArgumentException (
               "Datagram channel is null and the port number i zero");
      }

      // FIXME: This implementation has a decidedly simplistic
      // threading model. At least there should be something limiting
      // a storm of incoming packets to generate an excessive amount
      // of threads
      
      ByteBuffer bb = ByteBuffer.allocate (1500);
      for (;;)
      {
         SocketAddress a = ch.receive (bb);
         log.info ("Received initial datagram from " + a);
         bb.flip ();
         if (bb.limit () > 0)
         {
            DatagramChannel tsportCh =
               DatagramChannel.open ()
               .setOption (StandardSocketOptions.SO_REUSEADDR, true)
               .bind (ch.getLocalAddress ())
               .connect (a);

            tsportCh.configureBlocking (false);

            try
            {
               TransportSession tsport =
                  new TransportSession (tsportCh, bb, this);
               bb = ByteBuffer.allocate (1500);
               new Thread (tsport).start ();
            }
            catch (BlinkException e)
            {
               throw new XmitException (e);
            }
         }
         else
         {
            bb.clear ();
            log.warn ("%s: Empty initial packet from %s", info (ch), a);
         }
      }
   }

   @Override
   public void run ()
   {
      try
      {
         mainLoop ();
      }
      catch (Throwable e)
      {
         log.fatal (e, "%s: %s", info (ch), getInnerCause (e));
      }
   }

   @Override
   public Session resumeSession (Journal journal) throws IOException
   {
      UUID id = journal.getSessionId ();
      ServerSession s = new ServerSession (id, keepaliveInterval, om);
      s.resume (journal);
      sessionMap.put (id, s);
      return s;
   }
      
   private boolean hasSession (UUID id)
   {
      return sessionMap.containsKey (id);
   }

   void negotiate (xmit.Negotiate neg, TransportSession tsport)
   {
      UUID id = Util.toUuid (neg.getSessionId ());
      if (validate (neg, id, tsport))
      {
         ServerSession s = new ServerSession (id, keepaliveInterval, om);
         NegCtrlImpl ctrl = new NegCtrlImpl (id, neg, tsport);

         try
         {
            negObs.onNegotiate (ctrl, s);
         }
         catch (Exception e)
         {
            log.warn ("%s: onNegotiate: %s", info (ch), getInnerCause (e));
         }
         
         if (ctrl.wasAccepted ())
         {
            s.setJournal (ctrl.getJournal ());
            acceptNewSession (neg, id, s, tsport);
         }
         else
         {
            if (! ctrl.wasRejected ())
               tsport.sendNegRej (neg,
                                  xmit.NegotiationRejectCode.Credentials,
                                  "Not authorized");
         }
      }
   }

   private void acceptNewSession (xmit.Negotiate neg, UUID id, ServerSession s,
                                  TransportSession tsport)
   {
      sessionMap.put (id, s);
      xmit.NegotiationResponse resp = new xmit.NegotiationResponse ();
      resp.setSessionId (neg.getSessionId ());
      resp.setRequestTimestamp (neg.getTimestamp ());
      resp.setServerFlow (xmit.FlowType.Sequenced);

      if (log.isActiveAtLevel (Logger.Level.Trace))
         tsport.traceResponse (resp, resp.getSessionId (),
                               resp.getRequestTimestamp ());

      tsport.sendSafe (resp);
   }
   
   private boolean validate (xmit.Negotiate neg, UUID id,
                             TransportSession tsport)
   {
      if (hasSession (id))
      {
         tsport.sendNegRej (neg, xmit.NegotiationRejectCode.Unspecified,
                            "Already negotiated");
         return false;
      }

      if (neg.getClientFlow () != xmit.FlowType.Idempotent)
      {
         tsport.sendNegRej (neg,
                            xmit.NegotiationRejectCode.FlowTypeNotSupported,
                            "The flow type " + neg.getClientFlow () +
                            " is not supported");
         return false;
      }

      return true;
   }

   ServerSession establish (xmit.Establish est, TransportSession tsport)
   {
      UUID id = Util.toUuid (est.getSessionId ());
      ServerSession s = sessionMap.get (id);
      tsport.updateTimeout ((int) (s.getKeepaliveInterval () * 0.9));
      if (s != null && s.establish (est, tsport))
         return s;
      else
      {
         tsport.sendEstRej (est, xmit.EstablishmentRejectCode.Unnegotiated,
                            "Not negotiated");
         return null;
      }
   }

   ObjectModel getObjectModel ()
   {
      return om;
   }

   private final HashMap<UUID, ServerSession> sessionMap =
      new HashMap<UUID, ServerSession> ();
   private final ObjectModel om;
   private int keepaliveInterval;
   private NegotiationObserver negObs;
   private final int port;
   private DatagramChannel ch;
   private final Logger log =
      Logger.Manager.getLogger (com.pantor.xmit.Server.class);
}
