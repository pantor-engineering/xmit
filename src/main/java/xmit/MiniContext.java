// Generated by blinkc.js

package xmit;

public class MiniContext
{
  public java.lang.String getSessionIdPrefix () { return m_SessionIdPrefix; }
  public void setSessionIdPrefix (java.lang.String v) { m_SessionIdPrefix = v; }
  public int getNextSeqNo () { return m_NextSeqNo; }
  public boolean hasNextSeqNo () { return has_NextSeqNo; }
  public void clearNextSeqNo () { has_NextSeqNo = false; }
  public void setNextSeqNo (int v) { m_NextSeqNo = v; has_NextSeqNo = true; }
  
  private boolean has_NextSeqNo;
  
  private java.lang.String m_SessionIdPrefix;
  private int m_NextSeqNo;
}

