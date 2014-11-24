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

import java.nio.ByteBuffer;
import java.nio.channels.NetworkChannel;
import java.util.UUID;
import java.util.Collection;
import java.io.IOException;

public final class Util
{
   public static byte [] toBytes (UUID id)
   {
      byte [] bytes = new byte [16];
      ByteBuffer bb = ByteBuffer.wrap (bytes);
      bb.putLong (id.getMostSignificantBits ());
      bb.putLong (id.getLeastSignificantBits ());
      return bytes;
   }
   
   public static UUID toUuid (byte [] bytes)
   {
      ByteBuffer bb = ByteBuffer.wrap (bytes);
      long hi = bb.getLong ();
      long lo = bb.getLong ();
      return new UUID (hi, lo);
   }

   public static String getInnerCause (Throwable e)
   {
      while (e.getCause () != null)
         e = e.getCause ();
      return e.toString ();
   }

   public static long now ()
   {
      return System.currentTimeMillis ();
   }

   private static final long NanoEpoch =
      System.currentTimeMillis () * 1000000 - System.nanoTime ();
   
   public static long nowNano ()
   {
      return System.nanoTime () + NanoEpoch;
   }

   public static String nanoToStr (long tsp)
   {
      return String.format ("%tF %<tT.%09d", tsp / 1000000, tsp % 1000000000);
   }

   public static String getMsgType (Object o)
   {
      return o.getClass ().getName ();
   }

   public static int getIterableSize (Iterable<?> c)
   {
      if (c instanceof Collection<?>)
         return ((Collection<?>)c).size ();
      else
      {
         int count = 0;
         for (Object i : c)
            ++ count;
         return count;
      }
   }

   public static String info (NetworkChannel ch)
   {
      try
      {
         return ch.getLocalAddress ().toString ();
      }
      catch (IOException e)
      {
         return ch.toString ();
      }
   }
}
