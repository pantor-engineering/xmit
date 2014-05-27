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

import com.pantor.blink.NsName;
import com.pantor.blink.DefaultObjectModel;
import com.pantor.blink.ObjectModel;
import com.pantor.blink.Observer;
import com.pantor.blink.BlinkException;
import java.io.IOException;
import java.util.UUID;
import java.nio.channels.DatagramChannel;

public final class Client
{
   /**
      The flow type as defined in the XMIT specification
    */
   
   public enum FlowType
   {
      Idempotent, Unsequenced
   }
   
   /**
      Creates a session that will use the specified channel.

      <p>It will map messages as defined by the specified object model.</p>

      @param channel a connected DatagramChannel
      @param om a Blink object model
      @param obs a session event observer
      @param keepaliveInterval the client side keep alive interval in
      milliseconds
      @param flowType the flow type
      @throws XmitException if there is an Xmit problem
      @throws IOException if there is a socket problem
   */
      
   public static Session createSession (
      DatagramChannel channel, ObjectModel om, Session.EventObserver obs,
      int keepaliveInterval, FlowType flowType)
      throws IOException, XmitException
   {
      return new com.pantor.xmit.impl.ClientSession (
         channel, om, obs, keepaliveInterval, keepaliveInterval, flowType);
   }

   /**
      Creates a session that will use the specified channel.

      <p>It will map messages as defined by the specified object model.</p>

      @param channel a connected DatagramChannel
      @param om a Blink object model
      @param obs a session event observer
      @param keepaliveInterval the client side keep alive interval in
      milliseconds
      @param opTimeout the timeout for idempotent operations in milliseconds,
      the smallest effective timeout will be one second
      @param flowType the flow type
      @throws XmitException if there is an Xmit problem
      @throws IOException if there is a socket problem
   */
      
   public static Session createSession (
      DatagramChannel channel, ObjectModel om, Session.EventObserver obs,
      int keepaliveInterval, int opTimeout, FlowType flowType)
      throws IOException, XmitException
   {
      return new com.pantor.xmit.impl.ClientSession (
         channel, om, obs, keepaliveInterval, opTimeout, flowType);
   }
   
   /**
      Creates a session that will use the specified socket.

      <p>It will map messages as defined by the specified schemas.</p>

      @param channel a connected DatagramChannel
      @param schemas an array of Blink schema file names
      @param obs a session event observer
      @param keepaliveInterval the client side keep alive interval for
      the xmit session
      @throws XmitException if there is an Xmit problem
      @throws IOException if there is a socket problem
   */
      
   public static Session createSession (
      DatagramChannel channel, String [] schemas,
      Session.EventObserver obs, int keepaliveInterval, FlowType flowType)
      throws IOException, XmitException
   {
      try
      {
         return createSession (channel, new DefaultObjectModel (schemas), obs,
                               keepaliveInterval, flowType);
      }
      catch (BlinkException e)
      {
         throw new XmitException (e);
      }
   }

   public interface Session extends Runnable
   {
      /**
         Adds an observer for received application messages. 
         The prefix when looking up matching observer methods will be "on".

         @param obs an observer to add
         @throws XmitException if there is a schema or binding problem
      */
   
      void addAppObserver (Object obs) throws XmitException;

      /**
         Adds an application message observer.

         <p>The observer is added to an internal instance of the {@code
         com.pantor.blink.DefaultObsRegistry}.</p>

         @param obs the observer
         @param prefix the prefix used by the registry to identify
         observer methods
       */
         
      void addAppObserver (Object obs, String prefix) throws XmitException;

      /**
         Adds an application message observer for a specified message

         <p>The observer is added to an internal instance of the {@code
         com.pantor.blink.DefaultObsRegistry}.</p>

         @param name the type name of the message to observe
         @param obs the observer
       */
         
      void addAppObserver (NsName name, Observer obs);

      /**
         Initiate an xmit session using optional credentials. Any failure
         to initiate will be reported as failed negotiation or establishment
         through the {@code Session.EventObserver}

         @param credentials credentials to use when initiating. Use
         {@code null} to indicate absence of credentials.
      */
   
      void initiate (Object credentials);

      /**
         Initiate an xmit session. Any failure to initiate will be
         reported as failed negotiation or establishment through the
         {@code Session.EventObserver}
      */
   
      void initiate ();
   
      /**
         Terminate an xmit session

         @param reason the reason for terminating
      */

      void terminate (String reason);

      /**
         Resets the session. If the session is already established, it
         will be terminated.  If there are any ongoing negotiation or
         establishment attempts, they will be aborted. It will however
         retain any already achieved negotiation state.
      */
   
      void reset ();

      /**
         Resets the session with a reason specifier. If the session is already established, it
         will be terminated.  If there are any ongoing negotiation or
         establishment attempts, they will be aborted. It will however
         retain any already achieved negotiation state.
      */
   
      void reset (String reason);
   
      /**
         Sends an application message

         @param msg message to send
         @return the outgoing sequence number or zero if unsequenced
         @throws XmitException if there is a schema or binding problem
         @throws IOException if there is a socket problem
      */
   
      long send (Object msg) throws IOException, XmitException;

      /**
         Sends an array of messages

         @param msgs messages to send
         @return the outgoing sequence number of the first message or
         zero if unsequenced
         @throws XmitException if there is a schema or binding problem
         @throws IOException if there is a socket problem
      */

      long send (Object [] msgs) throws IOException, XmitException;

      /**
         Sends a slice of an array of messages

         @param msgs messages to send
         @param from the index of the first message to send
         @param len the number of objects to send
         @return the outgoing sequence number of the first message or
         zero if unsequenced
         @throws XmitException if there is a schema or binding problem
         @throws IOException if there is a socket problem
      */

      long send (Object [] msgs, int from, int len)
         throws IOException, XmitException;

      /**
         Sends an iterable collection of messages

         @param msgs the messages
         @return the outgoing sequence number of the first message or
         zero if unsequenced
         @throws XmitException if there is a schema or binding problem
         @throws IOException if there is a socket problem
      */

      long send (Iterable<?> msgs) throws IOException, XmitException;

      /**
         Resends an idempotent operation

         <p>This method is only applicable if the flow type is {@code
         FlowType.Idempotent}.</p>

         @param msg message to send
         @param seqNo operation sequence number, must be a sequence number
         of a previously sent operation
         @throws XmitException if there is a schema or binding problem
         @throws IOException if there is a socket problem
      */
   
      void resend (Object msg, long seqNo) throws IOException, XmitException;

      /**
         Resends an array of idempotent operations

         <p>This method is only applicable if the flow type is {@code
         FlowType.Idempotent}.</p>

         @param msgs messages to send
         @param firstSeqNo operation sequence number of the first message,
         must be a sequence number of a previously sent operation
         @throws XmitException if there is a schema or binding problem
         @throws IOException if there is a socket problem
      */

      void resend (Object [] msgs, long firstSeqNo)
         throws IOException, XmitException;

      /**
         Resends a slice of an array of idempotent operations

         <p>This method is only applicable if the flow type is {@code
         FlowType.Idempotent}.</p>
         
         @param msgs messages to send
         @param from the index of the first message to send
         @param len the number of objects to send
         @param firstSeqNo operation sequence number of the first message,
         must be a sequence number of a previously sent operation
         @throws XmitException if there is a schema or binding problem
         @throws IOException if there is a socket problem
      */

      void resend (Object [] msgs, int from, int len, long firstSeqNo)
         throws IOException, XmitException;

      /**
         Resends an iterable collection of messages

         <p>This method is only applicable if the flow type is {@code
         FlowType.Idempotent}.</p>

         @param msgs the messages
         @param firstSeqNo operation sequence number of the first message,
         must be a sequence number of a previously sent operation
         @throws XmitException if there is a schema or binding problem
         @throws IOException if there is a socket problem
      */

      void resend (Iterable<?> msgs, long firstSeqNo)
         throws IOException, XmitException;
      
      /**
         Returns the identifier of this session

         @return a uinque identifier of this session
      */
   
      UUID getSessionId ();

      /** Runs the session by calling {@code eventLoop}*/

      void run ();

      /** Starts the client by creating a new thread */

      void start ();

      /**
         Stops the session

         <p>If the session is established it will call terminate first.</p>
      */

      void stop ();
      
      /**
         Decodes incoming messages from the channel. It will run
         indefinitely and will only return if the channel is closed or if
         an exception occurs.

         @throws XmitException if there is a schema or binding problem
         @throws IOException if there is a socket or communications problem
      */

      void eventLoop () throws XmitException, IOException;
      
      public interface EventObserver
      {
         /**
            Called when an xmit session has been established

            @param session the xmit session being established
         */
   
         void onEstablished (Session session);

         /**
            Called when an xmit session establishment is rejected

            @param reason reject reason
         */
   
         void onEstablishmentRejected (String reason);

         /**
            Called when an xmit session negotionation is rejected

            @param reason reject reason
         */
   
         void onNegotiationRejected (String reason);

         /**
            Called if an xmit session establishment failes

            @param cause the exception causing the failure
         */
   
         void onEstablishmentFailed (Throwable cause);

         /**
            Called if an xmit session negotionation fails

            @param cause the exception causing the failure
         */
   
         void onNegotiationFailed (Throwable cause);

         /**
            Called when the session has been terminated

            @param reason the reason for the termination
            @param cause the exception that caused the termination. Can be
            {@code null}
         */

         void onTerminated (String reason, Throwable cause);

         /**
            Called when one or more operations have been applied by the
            server

            @param from the earliest seqno applied
            @param count the number of applied operations
         */

         void onOperationsApplied (long from, int count);

         /**
            Called when one or more operations will never be applied
            by the server.

            <p>If still applicable, a client may choose to resend the
            operations using fresh sequence numbers.</p>

            @param from the earliest seqno not applied
            @param count the number of not applied operations
         */

         void onOperationsNotApplied (long from, int count);

         /**
            Called when one or more operations has not been
            acknowledged as applied by the server within a timeout
            interval configured on the session.

            <p>Note that the operations may actualy have been applied,
            only that the acknowledgements got lost or delayed.</p>

            <p>If still applicable, a client should resend the same
            messages again using the same sequence numbers in order to
            guarantee at most once semantics.</p>

            @param from the earliest seqno timed out
            @param count the number of timed out operations
         */

         void onOperationsTimeout (long from, int count);
      }
   }
}
