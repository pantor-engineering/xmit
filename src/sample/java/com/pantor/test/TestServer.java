// Copyright (c), Pantor Engineering AB, 2013-
// All rights reserved

package com.pantor.test;

import com.pantor.blink.DefaultObjectModel;
import com.pantor.blink.Logger;
import com.pantor.blink.ConciseLogger;
import com.pantor.xmit.Server;
import com.pantor.xmit.InMemoryJournal;
import com.pantor.xmit.Server.NegotiationCtrl;
import com.pantor.xmit.Server.EstablishmentCtrl;
import com.pantor.xmit.XmitException;

public class TestServer implements Server.NegotiationObserver
{
   public static class Session implements Server.Session.EventObserver
   {
      Session (Server.Session sn)
      {
         this.sn = sn;
         sn.setEventObserver (this);
      }
      
      public void onPing (test.Ping ping)
      {
         try
         {
            int val = ping.getValue ();
            log.info ("Got ping (%d), sending pong (%d)", val, val);
            sn.send (new test.Pong (val));
         }
         catch (Throwable e)
         {
            while (e.getCause () != null)
               e = e.getCause ();
            log.error (e.toString ());
         }
      }

      public void onEstablish (EstablishmentCtrl e, Server.Session s)
      {
         log.info ("Session estblished");
      }

      public void onTerminate (String reason, Throwable cause, Server.Session s)
      {
         log.info ("Session terminated: %s", reason);
      }
      
      public void onFinished (Server.Session s)
      {
      }

      public void onDatagramStart (Server.Session s)
      {
      }
      
      public void onDatagramEnd (Server.Session s)
      {
      }
      
      private final Server.Session sn;
   }

   @Override 
   public void onNegotiate (NegotiationCtrl ctrl, Server.Session s)
      throws XmitException
   {
      s.addAppObserver (new Session (s));
      ctrl.accept (new InMemoryJournal (ctrl.getSessionId ()));
   }
   
   public static void main (String... args) throws Exception
   {
      DefaultObjectModel om = new DefaultObjectModel (args [0], args [1]);
      int port = Integer.parseInt (args [2]);
      Server s = Server.createServer (port, om, new TestServer ());
      log.info ("Starting server on port %d", port);
      s.run ();
   }

   private static final Logger.Level LogLevel = Logger.Level.Info;
   private static final int LogIndent = 30;
   
   static { Logger.Manager.setFactory (
         new ConciseLogger.Factory (LogLevel, LogIndent)); }

   private static Logger log = Logger.Manager.getLogger (TestClient.class);
}
