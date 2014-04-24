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

import com.pantor.blink.NsName;
import com.pantor.blink.DefaultObjectModel;
import com.pantor.blink.ObjectModel;
import com.pantor.blink.Observer;
import com.pantor.blink.BlinkException;
import java.nio.channels.DatagramChannel;
import java.io.IOException;
import java.net.SocketAddress;
import java.util.UUID;

/**
   The {@code Server} class provides an XMIT UDP server.

   <p>It accepts incoming requests for negotiating new sessions and
   establishing exisiting ones, using the XMIT protocol. The messages
   are encoded in the Blink compact binary format.</p>

   <p>You create new instances of the server by calling one of the
   {@code createServer} methods.</p>

   <p>If you have persisted earlier sessions you should initialize a
   fresh server through calls to the {@code resumeSession}.</p>

   <p>The server delegates incoming negotiation requests for new
   sessions to a NegotiationObserver. The observer is responsible for
   checking credentials and if the session is accepted, provide a
   journal instance.</p>
 */

public abstract class Server implements Runnable
{
   /**
      Creates a new server listening for incoming datagrams on the
      specified port.

      <p>Requests for negotiating new session will
      be forwarded to the specified negotiation observer.</P>

      @param port the port to listen to
      @param om a Blink object model
      @param negObs the negotiation observer
    */
   
   public static Server createServer (int port, ObjectModel om,
                                      NegotiationObserver negObs)
      throws IOException, XmitException
   {
      return new com.pantor.xmit.impl.Server (port, om, negObs);
   }

   /**
      Creates a new server listening for incoming datagrams on the
      specified port.

      <p>Requests for negotiating new session will
      be forwarded to the specified negotiation observer.</P>

      @param port the port to listen to
      @param schemas an array of Blink schema file names
      @param negObs the negotiation observer
    */
   
   public static Server createServer (int port, String [] schemas,
                                      NegotiationObserver negObs)
      throws IOException, XmitException
   {
      try
      {
         return createServer (port, new DefaultObjectModel (schemas), negObs);
      }
      catch (BlinkException e)
      {
         throw new XmitException (e);
      }
   }

   /**
      Creates a new server listening for incoming datagrams on the
      specified datagram channel.

      <p>Requests for negotiating new session will
      be forwarded to the specified negotiation observer.</P>

      @param ch the channel to listen to
      @param om the Blink object model
      @param negObs the negotiation observer
    */
   
   public static  Server createServer (DatagramChannel ch, ObjectModel om,
                                       NegotiationObserver negObs)
   {
      return new com.pantor.xmit.impl.Server (ch, om, negObs);
   }
   
   /**
      Sets the keep-alive interval used on sessions created by this
      server.

      <p>The keep-alive interval specifies how often the server
      will send a heartbeat message. If not set, the keep-alive
      interval is two seconds.</p>

      @param keepaliveInterval the interval expressed in milliseconds
    */
   
   public abstract void setKeepaliveInterval (int keepaliveInterval);

   /**
      Enters the main loop of the server.

      <p>The main loop will listen for incoming negotiate and
      establish requests.</p>

      <p>This method will never return normally.</p>

      @throws IOException if there is a socket problem
      @throws XmitException if there is a schema or binding problem
    */
   
   public abstract void mainLoop () throws XmitException, IOException;

   /**
      Resumes a session from the specified journal.

      <p>A resumed session is considered to be in a negotiated but not
      established state.</p>

      <p>The returned session object should typically be initialized
      by adding application message observers and a session state
      observer.</p>

      @param journal the journal to resume from
      @return the corresponding session object
    */
   
   public abstract Session resumeSession (Journal journal) throws IOException;

   /**
      Runs the {@code mainLoop}
   */
   
   @Override
   public abstract void run ();

   /**
      A represenation of a logical XMIT session.

      <p>The session provide methods for registring a state change
      event observer and observers for incoming application messages.</p>

      <p>The session also provides methods for sending messages back
      to the peer.</p>
    */

   public interface Session
   {
      /**
         Sends a single message back to the client

         @param msg the message
         @throws XmitException if there is a schema or binding problem
         @throws IOException if there is a socket problem
      */
      
      void send (Object msg) throws XmitException, IOException;

      /**
         Sends an array of messages back to the client

         @param msgs the messages
         @throws XmitException if there is a schema or binding problem
         @throws IOException if there is a socket problem
      */
      
      void send (Object [] msgs) throws XmitException, IOException;

      /**
         Sends a splice of an array of messages back to the client

         @param msgs messages to send
         @param from the index of the first message to send
         @param len the number of objects to send
         @throws XmitException if there is a schema or binding problem
         @throws IOException if there is a socket problem
      */
      
      void send (Object [] msgs, int from, int len)
         throws XmitException, IOException;
      
      /**
         Sends an iterable collection of messages back to the client

         @param msgs the messages
         @throws XmitException if there is a schema or binding problem
         @throws IOException if there is a socket problem
      */
      
      void send (Iterable<?> msgs) throws IOException, XmitException;

      /**
         Adds an application message observer.

         <p>The observer is added to an internal instance of the {@code
         com.pantor.blink.DefaultObsRegistry}.</p>

         @param obs the observer
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
         Sets an observer that will be notified about session state
         change events

         @param obs the session event observer
      */
      
      void setEventObserver (EventObserver obs);
      
      /**
         Notifies the session that the previous operation has been
         applied.

         <p>The application is responsible for calling this method if
         the apply mode of the server is {@code Manual}<p>

         <p>This method is only applicable when the flow type of the
         session is idempotent.</p>
      */
      
      void applied ();

      /**
         Terminates this transport

         @param reason a descriptive text of the termination reason
         @throws XmitException if there is a schema or binding problem
         @throws IOException if there is a socket problem
      */
      
      void terminate (String reason) throws XmitException, IOException;

      /**
         Indicates that this session is logically finished. This will
         eventually terminate the session.

         @throws XmitException if there is a schema or binding problem
         @throws IOException if there is a socket problem
      */

      void finish () throws XmitException, IOException;

      /**
         Tells the session to automatically mark operations as applied
         after the corresponding messages has been dispatched to the
         application.

         <p>If the auto-apply mode is disabled by calling this method
         with {@code false}, the application is responsible for
         conveying the information about applied operations to the
         client. As an alternative, the application can choose to call
         the {@code Session.applied} method explicitly in this
         mode. By doing so, the session will send an Xmit:Applied
         message just like in the auto-apply mode.</p>

         <p>The auto-apply mode is only applicable to sessions using
         the idempotent flow type.</p>

         <p>The default mode is that auto-apply is enabled.</p>

         @param useAutoApply the state
      */
   
      void setUseAutoApply (boolean useAutoApply);

      public interface EventObserver
      {
         /**
            Notifies the observer that the session has been bound to a
            new transport.

            <p>The observer has the ability to reject the binding by
            calling {@code e.reject ()}.</p>

            <p>Unless explicitly rejected through {@code e.reject ()},
            the establish request will be accepted.</p>

            @param req an establishment request
            @param s the session
         */
      
         void onEstablish (EstablishmentRequest req, Session s);

         /**
            Notifies the observer that the transport has been
            terminated by the peer
         
            <p>Unless the session has been finished on the logical
            level, the session may later be resumed again with a new
            transport and a corresponding call to {@code
            onEstablish}</p>

            @param reason the reason for the termination
            @param cause the exception that caused the termination. Can be
            {@code null}
            @param s the session
         */
      
         void onTerminate (String reason, Throwable cause, Session s);

         /**
            Notifies the observer that the session is logically finished
            and cannot be resumed again

            @param s the session
         */
      
         void onFinished (Session s);

         /**
            Notifies the observer at the start of a datagram

            @param s the session
         */
      
         void onDatagramStart (Session s);
      
         /**
            Notifies the observer at the end of a datagram

            @param s the session
         */
      
         void onDatagramEnd (Session s);
      }
   }

   /**
      The negotiation request interface provides means for accepting and
      rejecting new logical XMIT sessions
    */

   public interface NegotiationRequest
   {
      /**
         Indicates that the negotiation request was accepted.

         <p>The negotiation will be rejected unless this method is called.</p>
      */
      
      void accept (Journal journal);

      /**
         Inidicates that the negotiation was rejected due to invalid
         or incomplete credentials.

         @param reason a string describing the cause
       */
      
      void rejectCredentials (String reason);

      /**
         Inidicates that the negotiation was rejected due to some cause
         that cannot be attributed to the credentials.

         @param reason a string describing the cause
       */
      
      void rejectOther (String reason);

      /**
         Returns the credentials

         @return the credential object which may be {@code null}
      */

      Object getCredentials ();

      /**
         Returns the source address of the negotiation request

         @return the source address
      */
      
      SocketAddress getSourceAddress () throws IOException;

      /**
         Returns the unique identifier for the requested session

         @return the session identifier
       */

      UUID getSessionId ();

      /**
         Returns the timestamp of the negotiation request

         @return nanoseconds since 1970-01-01 00:00 UTC
       */
      
      long getRequestTimestamp ();
   }

   /**
      The establishment request interface provides means for accepting
      and rejecting bindings of logical XMIT sessions to new
      transports.
    */

   public interface EstablishmentRequest
   {
      /**
         Indicates that the establishment request was accepted.

         <p>A call to this method is not strictly needed since any
         establishment request that is not explicitly rejected will be
         considered accepted.</p>
      */
      
      void accept ();

      /**
         Inidicates that the establishment was rejected due to invalid
         or incomplete credentials.

         @param reason a string describing the cause
       */
      
      void rejectCredentials (String reason);

      /**
         Inidicates that the negotiation was rejected due to some cause
         that cannot be attributed to the credentials.

         @param reason a string describing the cause
       */
      
      void rejectKeepaliveInterval (String reason);

      /**
         Inidicates that the negotiation was rejected due to some
         cause that cannot be attributed to the credentials or keep
         alive interval

         @param reason a string describing the cause
       */
      
      void rejectOther (String reason);

      /**
         Returns the credentials

         @return the credential object which may be {@code null}
      */

      Object getCredentials ();

      /**
         Returns the keep alive interval

         @return the keep alive interval in milliseconds
      */
      
      int getClientKeepaliveInterval ();

      /**
         Returns the source address of the establishment request

         @return the source address
      */
      
      SocketAddress getSourceAddress () throws IOException;
   }

   /**
      A negotiation observer will receive notifications of incoming
      requests for creating new logical XMIT session.
     
      <p>An implementaion of this interface must be passed to the {@code
      Server.createServer} factory method.</p>
    */
   
   public interface NegotiationObserver
   {
      /**
         Notifies the observer that a new session has been requested.
         
         <p>The observer should validate any credentials supplied in
         the negotiation request and then call {@code n.accept ()}
         or {@code n.rejectNNN (...)} depending on the outcome.</p>

         <p>If the observer does nothing, the negotiation request will
         be rejected assuming that the credential check failed.</p>

         <p>If the session is accepted, the observer must supply a
         journal to the session</p>
         
         <p>If the session is accepted, the implementaion of {@code
         onNegotiate} typically initializes the session by adding
         application message observers and a session state observer.</p>

         @param req a negotiation request
         @param s the session
       */

      void onNegotiate (NegotiationRequest req, Session s)
         throws IOException, XmitException;
   }

   public interface Journal
   {
      /**
         Returns the unique session id

         @return the session identifier
       */

      UUID getSessionId () throws IOException;

      /**
         Returns the sequence number of the last outgoing message
         stored in this journal

         @return the last outgoing sequence number, or zero if the
         journal is empty
         @throws IOException if there is a read problem
      */
      
      long getLastOutgoingSeqNo () throws IOException;

      /**
         Returns the sequence number of the last incoming operation

         @return the last incoming sequence number, or zero if no messages
         has been received
         @throws IOException if there is a read problem
       */

      long getLastIncomingSeqNo () throws IOException;

      /**
         Writes a single message to the journal

         <p>The call must write a complete message or fail with an
         exception.</p>

         @param seqNo the sequence number of the message
         @param msg the message
         @throws IOException if there is a write problem
      */
      
      void write (long seqNo, Object msg) throws IOException;

      /**
         Write a single message to the journal together with the current
         incoming sequence number

         <p>The call must write a complete message and persist the
         incoming sequence number or fail with an exception.</p>

         @param outSeqNo the sequence number of the message
         @param inSeqNo the sequence number of the last applied
         incoming message
         @param msg the message
         @throws IOException if there is a write problem
       */
      
      void write (long outSeqNo, long inSeqNo, Object msg) throws IOException;

      /**
         Writes a splice of an array ot the journal

         <p>The call must fully write all the messages or fail with an
         exception.</p>

         @param seqNo the sequence number of the first message
         @param msgs messages to write
         @param from the index of the first message to write
         @param len the number of objects to write
         @throws IOException if there is a write problem
      */
      
      void write (long seqNo, Object [] msgs, int from, int len)
         throws IOException;
      
      /**
         Writes an iterable collection of messages to the journal.

         <p>The call must fully write all the messages or fail with an
         exception.</p>

         @param seqNo the sequence number of the first message
         @param msgs the messages
         @throws IOException if there is a write problem
      */
      
      void write (long seqNo, Iterable<?> msgs) throws IOException;

      /**
         Reads a sequence of message from the journal

         @param from the sequence number of the first message to read
         @param count the number of messages to read
         @param msgs the array to store the read messages in
         @throws IOException if there is a read problem
       */

      void read (long from, int count, Object [] msgs) throws IOException;
      
      /**
         Notifies the journal that the session is finnished and will
         never be resumed again.
      */

      void finished ();
   }
   
}
