XMIT defines lightweight logical sessions. A session is used
to carry application messages. XMIT is responsible for using
the selected communications mechanism to deliver messages
orderly from one application to one or more other applications
in the context of a session.

XMIT is designed to meet the requirements of automated trading
in the financial markets.  These include the use of open
protocols, simple design, freedom from being tied to a
particular messaging library implementation, and eliminating
unnecesssary overhead that affects communications latency.

This is a Java implementation of the XMIT protocol by Pantor
Engineering AB (http://www.pantor.com).
The XMIT specification can be found at http://blinkprotocol.org.

BUILD
=====

gradle build
