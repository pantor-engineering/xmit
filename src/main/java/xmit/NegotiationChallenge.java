// Generated by blinkc.js

package xmit;

public class NegotiationChallenge
{
  public byte [] getSessionId () { return m_SessionId; }
  public void setSessionId (byte [] v) { m_SessionId = v; }
  public long getRequestTimestamp () { return m_RequestTimestamp; }
  public void setRequestTimestamp (long v) { m_RequestTimestamp = v; }
  public long getTimestamp () { return m_Timestamp; }
  public void setTimestamp (long v) { m_Timestamp = v; }
  public java.lang.Object getChallenge () { return m_Challenge; }
  public void setChallenge (java.lang.Object v) { m_Challenge = v; }
  
  private byte [] m_SessionId;
  private long m_RequestTimestamp;
  private long m_Timestamp;
  private java.lang.Object m_Challenge;
}

