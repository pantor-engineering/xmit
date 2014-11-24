// Copyright (c), Pantor Engineering AB, 2013-
// All rights reserved

package com.pantor.test;

import com.pantor.blink.DefaultObjectModel;
import com.pantor.blink.Logger;
import com.pantor.blink.ConciseLogger;
import com.pantor.xmit.Client;
import com.pantor.xmit.Client.Session;
import java.net.InetSocketAddress;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SocketChannel;
import xmit.StringIdentification;

public class TestClient implements Session.EventObserver
{
   private final static int Sec = 1000;
   private final static int Keepalive = 2 * Sec;

   private static test.Ping makePing (int val, int count)
   {
      test.Ping ping = new test.Ping ();
      ping.setValue (val);
      ping.setPongCount (count);
      return ping;
   }

   public void onPong (test.Pong pong)
   {
      try
      {
         int val = pong.getValue ();
         log.info ("Got pong (%d), sending ping (%d)", val, val + 1);
         Thread.sleep (1000);
         long seqNo = sn.send (makePing (val + 1, 1));
         log.info ("  Sent ping with seqNo: %d", seqNo);
      }
      catch (Throwable e)
      {
         log.error (getInnerCause (e));
      }
   }

   public void onEstablished (Session session)
   {
      log.info ("Sesssion negotiated and established");
      log.info ("Sending first ping (1)");
      try
      {
         sn.send (makePing (1, 1));
      }
      catch (Throwable e)
      {
         log.error (getInnerCause (e));
      }
   }

   private static String getInnerCause (Throwable e)
   {
      while (e.getCause () != null)
         e = e.getCause ();
      return e.toString ();
   }

   public void onEstablishmentRejected (String reason)
   {
      log.info ("onEstablishmentRejected: " + reason);
   }

   public void onNegotiationRejected (String reason)
   {
      log.info ("onNegotiationRejected: " + reason);
      sn.stop (); 
   }

   public void onEstablishmentFailed (Throwable cause)
   {
      log.info ("onEstablishmentFailed: " + cause);
   }
   
   public void onNegotiationFailed (Throwable cause)
   {
      log.info ("onNegotiationFailed: " + cause);
      sn.stop (); 
   }

   public void onTerminated (String reason, Throwable cause)
   {
      log.info ("onTerminated: " + reason);
   }

   public void onOperationsApplied (long from, int count)
   {
      logOpEvent ("Applied", from, count);
   }

   public void onOperationsNotApplied (long from, int count)
   {
      logOpEvent ("Not applied", from, count);
   }
   
   public void onOperationsTimeout (long from, int count)
   {
      logOpEvent ("Operation timed out, resending", from, count);
      try
      {
         sn.resend (makePing ((int)from, 1), from);
      }
      catch (Throwable e)
      {
         log.error (getInnerCause (e));
      }
   }

   public void logOpEvent (String what, long from, int count)
   {
      if (count > 0)
      {
         if (count > 1)
            log.info ("%s %d .. %d", what, from, from + count - 1);
         else
            log.info ("%s %d", what, from);
      }
   }
   
   public static void main (String... args) throws Exception
   {
      DefaultObjectModel om = new DefaultObjectModel (args [0], args [1]);
      String [] parts = args [2].split (":");
      InetSocketAddress a =
         new InetSocketAddress (parts [0], Integer.parseInt (parts [1]));

      
      TestClient c = new TestClient ();

      if (args.length > 3 && args [3].equals ("tcp"))
      {
         SocketChannel ch = SocketChannel.open ();
         ch.socket ().connect (a);
         c.sn = Client.createSession (ch, om, c, Keepalive,
                                      Client.FlowType.Idempotent);
      }
      else
      {
         DatagramChannel ch = DatagramChannel.open ().connect (a);
         c.sn = Client.createSession (ch, om, c, Keepalive,
                                      Client.FlowType.Idempotent);
      }
         
      c.sn.addAppObserver (c);
      StringIdentification credentials = new StringIdentification ();
      credentials.setIdentity ("TestClient");
      c.sn.initiate (credentials);
      c.sn.start ();
      log.info ("Started client session");
   }

   private Session sn;
   private static final Logger.Level LogLevel = Logger.Level.Info;
   static { Logger.Manager.setFactory (new ConciseLogger.Factory (LogLevel)); }
   private static Logger log = Logger.Manager.getLogger (TestClient.class);
}
