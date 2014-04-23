// Copyright (c), Pantor Engineering AB, 2013-
// All rights reserved

package test;

public final class Ping
{
   public Ping () { }
   public Ping (int msg) { value = msg; } 
   public void setValue (int msg) { value = msg; }
   public int getValue () { return value; }

   private int value;
}
