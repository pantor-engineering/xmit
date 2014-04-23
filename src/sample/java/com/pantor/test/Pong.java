// Copyright (c), Pantor Engineering AB, 2013-
// All rights reserved

package test;

public final class Pong
{
   public Pong () { }
   public Pong (int msg) { value = msg; } 
   public void setValue (int msg) { value = msg; }
   public int getValue () { return value; }

   private int value;
}
