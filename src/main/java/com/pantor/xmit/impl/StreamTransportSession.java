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

import java.nio.channels.SocketChannel;

import com.pantor.blink.ObjectModel;
import com.pantor.blink.Logger;
import com.pantor.blink.DefaultObsRegistry;
import com.pantor.blink.CompactReader;
import com.pantor.blink.CompactWriter;
import com.pantor.blink.ByteBuf;
import com.pantor.blink.Dispatcher;
import com.pantor.blink.Observer;
import com.pantor.blink.BlinkException;

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

public final class StreamTransportSession extends TransportSession
{
   StreamTransportSession (SocketChannel ch, Server srv)
      throws BlinkException
   {
      super (TransportType.Stream, srv);
      this.ch = ch;
      this.inBb = ByteBuffer.allocate (4096);
      this.inBuf = new ByteBuf (inBb.array (), 0, inBb.limit ());
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
         log.error (e, "%s: %s", info (ch), getInnerCause (e));;
      }
   }

   @Override
   String getInfo ()
   {
      return info (ch);
   }
   
   private void eventLoop () throws IOException
   {
      try
      {
         long effectiveTimeout = timeout / 8;
         Selector selector = Selector.open ();
         ch.register (selector, SelectionKey.OP_READ);
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
                     i.remove();
                     if (k.isReadable ())
                        readChunk ();
                  }
               }
               catch (Throwable e)
               {
                  log.warn ("%s: Terminating transport session: %s",
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

         if (! rd.isComplete ())
            log.warn ("%s: Incomplete Blink content at end of stream",
                      info (ch));
         
      }
      finally
      {
         ch.close ();
         done = true;
      }
   }

   private void readChunk () throws BlinkException, IOException
   {
      inBb.clear ();
      ch.read (inBb);
      inBb.flip ();

      if (inBb.limit () > 0)
      {
         inBuf.clear ();
         inBuf.setPos (inBb.limit ());
         inBuf.flip ();
         decodeChunk ();
      }
   }

   private void decodeChunk () throws BlinkException, IOException
   {
      ServerSession s = xmitSession;
      if (s != null)
         s.onChunkStart ();
      rd.read (inBuf);
   }

   @Override
   void flush () throws IOException
   {
      outBuf.flip ();
      ch.write (outBuf.getByteBuffer ());
      outBuf.clear ();
   }

   @Override
   SocketAddress getSourceAddress () throws IOException
   {
      return ch.getRemoteAddress ();
   }

   private final SocketChannel ch;
   private final ByteBuffer inBb;
   private final ByteBuf inBuf;
}
