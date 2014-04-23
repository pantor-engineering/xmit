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

package com.pantor.xmit;

import java.util.UUID;
import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
import java.io.IOException;

public final class InMemoryJournal implements Server.Journal 
{
   public InMemoryJournal (UUID sessionId)
   {
      this.sessionId = sessionId;
   }
   
   @Override
   public UUID getSessionId ()
   {
      return sessionId;
   }

   @Override
   public long getLastOutgoingSeqNo ()
   {
      return lastOutgoingSeqNo;
   }

   @Override
   public long getLastIncomingSeqNo ()
   {
      return lastIncomingSeqNo;
   }

   @Override
   public void write (long seqNo, Object msg) throws IOException
   {
      if (seqNo == lastOutgoingSeqNo + 1)
      {
         ++ lastOutgoingSeqNo;
         store.add (msg);
      }
      else
         throw new IOException ("Journal gap detected: " + seqNo + " != " +
                                (lastOutgoingSeqNo + 1));
   }

   @Override
   public void write (long outSeqNo, long inSeqNo, Object msg)
      throws IOException
   {
      if (outSeqNo == lastOutgoingSeqNo + 1)
      {
         ++ lastOutgoingSeqNo;
         store.add (msg);
      }
      else
         throw new IOException ("Journal gap detected: " + outSeqNo + " != " +
                                (lastOutgoingSeqNo + 1));

      if (inSeqNo > lastIncomingSeqNo)
         lastIncomingSeqNo = inSeqNo;
   }

   @Override
   public void write (long seqNo, Object [] msgs, int from, int len)
      throws IOException
   {
      if (seqNo == lastOutgoingSeqNo + 1)
      {
         lastOutgoingSeqNo += len;
         List<Object> all = Arrays.asList (msgs);
         store.addAll (all.subList (from, from + len));
      }
      else
         throw new IOException ("Journal gap detected: " + seqNo + " != " +
                                (lastOutgoingSeqNo + 1));
   }
      
   @Override      
   public void write (long seqNo, Iterable<?> msgs) throws IOException
   {
      int pos = 0;
      for (Object msg : msgs)
         write (seqNo + (pos ++), msg);
   }

   @Override
   public void read (long from, int count, Object [] msgs)
   {
      -- from; // First message is stored at index 0
      store.subList ((int)from, (int)from + count).toArray (msgs);
   }
      
   public void finished ()
   {
      // Do nothing
   }

   private final UUID sessionId;
   private long lastOutgoingSeqNo;
   private long lastIncomingSeqNo;
   private final ArrayList<Object> store = new ArrayList<Object> ();
}
