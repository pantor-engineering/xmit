// Copyright (c), Pantor Engineering AB, 2013-
// All rights reserved

package com.pantor.xmit;

import java.util.Arrays;
import java.io.*;
import java.net.*;

import com.pantor.xmit.Session;
import com.pantor.xmit.XmitException;

import com.pantor.blink.Decimal;

import pmap.InsertOrder;
import pmap.OrderProps;
import pmap.MutableOrderProps;
import pmap.StdQuantity;
import pmap.LimitPriceType;
import pmap.TimeInForce;
import pmap.Side;
import pmap.OrderResponse;

public class TestClient implements SessionEventObserver
{
   public TestClient (String host, int port,
                      String [] schemas) throws IOException, XmitException
   {
      System.out.println ("Creating TestClient");

      DatagramSocket socket = new DatagramSocket ();
      socket.connect (new InetSocketAddress (host, port));

      session = new Session (socket, schemas, this, 1000, 1);

      session.addAppObserver (this);

      session.initiate (null);
      session.start ();
   }

   public void onOrderResponse (OrderResponse o)
   {
      System.err.println ("Got OrderResponse");
   }

   public void onEstablished (Session s)
   {
      System.out.println ("Session established");

      try
      {
         OrderProps op = new OrderProps ();
         MutableOrderProps mp = new MutableOrderProps ();

         LimitPriceType lp = new LimitPriceType ();
         lp.setPrice (new Decimal (10));

         StdQuantity qty = new StdQuantity ();
         qty.setOrderQty (500);

         op.setSide (Side.Buy);
         op.setTradableId ("MLT");
         op.setMutableProps (mp);

         mp.setQuantity (qty);
         mp.setPriceType (lp);
         mp.setTimeInForce (TimeInForce.Day);

         InsertOrder o = new InsertOrder ();
         o.setClientOrderId (1);
         o.setOrderProps (op);

         session.send (o);
      }
      catch (XmitException e)
      {
         System.err.println ("ERROR: " + e.getMessage ());
         e.printStackTrace ();
         System.exit (1);
      }
      catch (IOException e)
      {
         System.err.println ("ERROR: " + e.getMessage ());
         e.printStackTrace ();
         System.exit (1);
      }
   }

   public void onRejected (String reason)
   {
      System.out.println ("Session rejected: " + reason);
      System.exit (1);
   }

   public static void main (String... args) throws Exception
   {
      if (args.length < 3)
      {
         System.out.println (
            "Usage: TestClient <host> <port> <schema> [schema]");
         System.exit (1);
      }

      try
      {
         new TestClient (args [0], Integer.parseInt (args [1]),
                         Arrays.copyOfRange (args, 2, args.length));
      }
      catch (Exception e)
      {
         System.err.println ("ERROR: " + e.getMessage ());

         e.printStackTrace ();
      }
   }

   private final Session session;
}
