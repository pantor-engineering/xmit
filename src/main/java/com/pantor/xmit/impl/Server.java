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
import java.nio.channels.SocketChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.Selector;
import java.nio.channels.SelectionKey;
import java.nio.ByteBuffer;

import java.util.HashMap;
import java.util.UUID;
import java.util.Set;
import java.util.Iterator;

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
      this.dgmCh = ch;
   }

   public Server (ServerSocketChannel ch, ObjectModel om,
                  NegotiationObserver negObs)
   {
      this.om = om;
      this.negObs = negObs;
      this.port = 0;
      this.acceptCh = ch;
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
      if (dgmCh == null)
      {
         if (port > 0)
         {
            dgmCh =
               DatagramChannel.open ()
               .setOption (StandardSocketOptions.SO_REUSEADDR, true);
            dgmCh.bind (new InetSocketAddress (port));
         }
         else
         {
            if (acceptCh == null)
               throw new IllegalArgumentException (
                  "Datagram and socket channels are null and " +
                  "the port number i zero");
         }
      }

      if (acceptCh == null && port > 0)
      {
         acceptCh = ServerSocketChannel.open ();
         acceptCh.socket ().bind (new InetSocketAddress (port));
      }

      if (dgmCh != null)
         dgmCh.configureBlocking (false);

      if (acceptCh != null)
         acceptCh.configureBlocking (false);
      
      // FIXME: This implementation has a decidedly simplistic
      // threading model. At least there should be something limiting
      // a storm of incoming packets to generate an excessive amount
      // of threads
      
      Selector selector = Selector.open ();
      if (dgmCh != null)
         dgmCh.register (selector, SelectionKey.OP_READ);
      if (acceptCh != null)
         acceptCh.register (selector, SelectionKey.OP_ACCEPT);

      for (;;)
      {
         int n = selector.select ();
         if (n > 0)
         {
            Set<SelectionKey> keys = selector.selectedKeys ();
            Iterator<SelectionKey> i = keys.iterator();
            while (i.hasNext ())
            {
               SelectionKey k = i.next ();
               i.remove();
               if (k.isAcceptable ())
                  acceptConnection ();
               else
               {
                  if (k.isReadable ())
                     receivePacket ();
               }
            }
         }
      }
   }

   private void receivePacket () throws IOException, XmitException
   {
      ByteBuffer bb = ByteBuffer.allocate (1500);
      SocketAddress a = dgmCh.receive (bb);
      log.info ("Received initial datagram from " + a);
      bb.flip ();
      if (bb.limit () > 0)
      {
         DatagramChannel tsportCh =
            DatagramChannel.open ()
            .setOption (StandardSocketOptions.SO_REUSEADDR, true)
            .bind (dgmCh.getLocalAddress ())
            .connect (a);

         tsportCh.configureBlocking (false);

         try
         {
            new Thread (new DatagramTransportSession (tsportCh, bb,
                                                      this)).start ();
         }
         catch (BlinkException e)
         {
            throw new XmitException (e);
         }
      }
      else
         log.warn ("%s: Empty initial packet from %s", info (dgmCh), a);
   }

   private void acceptConnection () throws IOException, XmitException
   {
      SocketChannel socket = acceptCh.accept ();
      socket.configureBlocking (false);

      log.info ("Acceped connection from " + socket.getRemoteAddress ());
      
      try
      {
         new Thread (new StreamTransportSession (socket, this)).start ();
      }
      catch (BlinkException e)
      {
         throw new XmitException (e);
      }
   }

   private String getInfo ()
   {
      if (dgmCh != null)
         return info (dgmCh);
      else if (acceptCh != null)
         return info (acceptCh);
      else
         return "?";
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
         log.fatal (e, "%s: %s", getInfo (), getInnerCause (e));
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
         NegReqImpl req = new NegReqImpl (id, neg, tsport);

         try
         {
            negObs.onNegotiate (req, s);
         }
         catch (Exception e)
         {
            log.warn ("%s: onNegotiate: %s", getInfo (), getInnerCause (e));
         }
         
         if (req.wasAccepted ())
         {
            s.setJournal (req.getJournal ());
            acceptNewSession (neg, id, s, tsport);
         }
         else
         {
            if (! req.wasRejected ())
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
      if (s != null)
      {
         if (s.establish (est, tsport))
         { 
            tsport.updateTimeout ((int) (s.getKeepaliveInterval () * 0.9));
            return s;
         }
      }
      else
      {
         tsport.sendEstRej (est, xmit.EstablishmentRejectCode.Unnegotiated,
                            "Not negotiated");
      }
      
      return null;
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
   private DatagramChannel dgmCh;
   private ServerSocketChannel acceptCh;
   private final Logger log =
      Logger.Manager.getLogger (com.pantor.xmit.Server.class);
}
