**NOTE: Currently the new server functionality doesn't work properly on Windows**

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

Build
=====

In order to build xmit you must first make sure gradle can find a `jblink.jar`. A straightforward way is to place a copy of `jblink.jar` in the top-level `xmit` directory. Then you can build xmit by running gradle like this:

	gradle build

Example
=======

A sample server and client is available in the sample directory. You can build them by running gradle like this:

	gradle build sample

The following command line shows how to start the sample server on port 4711:

	java -cp jblink.jar:build/libs/xmit.jar:build/classes/sample com.pantor.test.TestServer src/sample/pingpong.blink xmit.blink 4711

And this will start the client:

	java -cp jblink.jar:build/libs/xmit.jar:build/classes/sample com.pantor.test.TestServer src/sample/pingpong.blink xmit.blink localhost:4711


