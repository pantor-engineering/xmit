// Generated by blinkc.js

package xmit;

public enum FlowType
{
  Sequenced (0),
  Unsequenced (1),
  Idempotent (2);
  
  private FlowType (int val) { this.val = val; }
  public int getValue () { return val; }
  private final int val;
}

