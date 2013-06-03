// Copyright (c), Pantor Engineering AB, 2013-
// All rights reserved

package com.pantor.xmit;

public class XmitException extends Exception
{
   public XmitException (String what)
   {
      super (what);
   }

   public XmitException (Throwable cause)
   {
      super (cause);
   }
}

