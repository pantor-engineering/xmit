// Generated by blinkc.js

package xmit;

public class Context
{
  public byte [] getSessionId () { return m_SessionId; }
  public void setSessionId (byte [] v) { m_SessionId = v; }
  public long getNextSeqNo () { return m_NextSeqNo; }
  public boolean hasNextSeqNo () { return has_NextSeqNo; }
  public void clearNextSeqNo () { has_NextSeqNo = false; }
  public void setNextSeqNo (long v) { m_NextSeqNo = v; has_NextSeqNo = true; }
  
  private boolean has_NextSeqNo;
  
  private byte [] m_SessionId;
  private long m_NextSeqNo;
}

