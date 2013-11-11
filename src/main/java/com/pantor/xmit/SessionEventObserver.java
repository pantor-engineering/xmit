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

import com.pantor.xmit.Session;

/**
   The {@code SessionEventObserver} interface provides an observer
   interface to session related events.
 */

public interface SessionEventObserver
{
   /**
      Called when an xmit session has been established

      @param session the xmit session being established
   */
   
   public void onEstablished (Session session);

   /**
      Called when an xmit session establishment is rejected

      @param reason reject reason
   */
   
   public void onEstablishmentRejected (String reason);

   /**
      Called when an xmit session negotionation is rejected

      @param reason reject reason
   */
   
   public void onNegotiationRejected (String reason);

   /**
      Called if an xmit session establishment failes

      @param cause the exception causing the failure
   */
   
   public void onEstablishmentFailed (Throwable cause);

   /**
      Called if an xmit session negotionation fails

      @param cause the exception causing the failure
   */
   
   public void onNegotiationFailed (Throwable cause);

   /**
      Called when the session has been terminated

      @param reason the reason for the termination
      @param cause the exception that caused the termination. Can be
      {@code null}
    */

   public void onTerminated (String reason, Throwable cause);

   /**
      Called when one or more operations sent by Session.sendOnce have
      been applied

      @param from the earliest seqno applied
      @param to the latest seqno applied
    */

   public void onAppliedOnce (int from, int to);

   /**
      Called when one or more operations sent by Session.sendOnce were
      not applied

      @param from the earliest seqno not applied
      @param to the latest seqno not applied
    */

   public void onNotAppliedOnce (int from, int to);
   
}
