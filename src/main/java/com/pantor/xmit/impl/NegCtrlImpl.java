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

import java.util.UUID;
import java.io.IOException;
import java.net.SocketAddress;
import com.pantor.xmit.Server.Journal;

final class NegCtrlImpl implements com.pantor.xmit.Server.NegotiationCtrl
{
   NegCtrlImpl (UUID id, xmit.Negotiate req, TransportSession tsport)
   {
      this.id = id;
      this.req = req;
      this.tsport = tsport;
   }
      
   @Override
   public void accept (Journal journal)
   {
      assert ! rejected;
      accepted = true;
      this.journal = journal;
   }

   @Override
   public void rejectCredentials (String reason)
   {
      assert ! accepted;
      assert ! rejected;
      rejected = true;
      tsport.sendNegRej (req,
                         xmit.NegotiationRejectCode.Credentials,
                         reason);
   }

   @Override
   public void rejectOther (String reason)
   {
      assert ! accepted;
      assert ! rejected;
      rejected = true;
      tsport.sendNegRej (req,
                         xmit.NegotiationRejectCode.Unspecified,
                         reason);
   }

   @Override
   public Object getCredentials ()
   {
      return req.getCredentials ();
   }
      
   @Override
   public SocketAddress getSourceAddress () throws IOException
   {
      return tsport.getSourceAddress ();
   }

   @Override
   public UUID getSessionId ()
   {
      return id;
   }

   @Override
   public long getRequestTimestamp ()
   {
      return req.getTimestamp ();
   }

   boolean wasAccepted ()
   {
      return accepted;
   }

   boolean wasRejected ()
   {
      return rejected;
   }

   Journal getJournal ()
   {
      return journal;
   }

   private final UUID id;
   private final xmit.Negotiate req;
   private final TransportSession tsport;
   private Journal journal;
   private boolean accepted;
   private boolean rejected;
}
