// Generated by blinkc.js

package xmit;

public class Negotiate
{
  public long getTimestamp () { return m_Timestamp; }
  public void setTimestamp (long v) { m_Timestamp = v; }
  public java.lang.String getSessionId () { return m_SessionId; }
  public void setSessionId (java.lang.String v) { m_SessionId = v; }
  public FlowType getClientFlow () { return m_ClientFlow; }
  public void setClientFlow (FlowType v) { m_ClientFlow = v; }
  public java.lang.Object getCredentials () { return m_Credentials; }
  public boolean hasCredentials () { return m_Credentials != null; }
  public void clearCredentials () { m_Credentials = null; }
  public void setCredentials (java.lang.Object v) { m_Credentials = v; }
  
  private long m_Timestamp;
  private java.lang.String m_SessionId;
  private FlowType m_ClientFlow;
  private java.lang.Object m_Credentials;
}
