// Generated by blinkc.js

package xmit_once;

public enum NotAppliedReason
{
  OutOfOrder (0),
  AlreadyApplied (1);
  
  private NotAppliedReason (int val) { this.val = val; }
  public int getValue () { return val; }
  private final int val;
}

