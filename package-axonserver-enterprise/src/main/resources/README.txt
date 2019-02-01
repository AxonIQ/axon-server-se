This is the Axon Server Enterprise Edition, version 4.0

For information about the Axon Framework and Axon Server,
visit https://docs.axoniq.io.

Running Axon Server
===================

By default the Axon Framework is configured to expect a running Axon Server
instance, and it will complain if the server is not found. To run Axon Server,
you'll need a Java runtime (JRE versions 8 through 10 are currently supported,
Java 11 still has Spring-boot related growing-pains).  A copy of the server
JAR file has been provided in the demo package. You can run it locally, in a
Docker container (including Kubernetes or even Minikube), or on a separate
server.

Running Axon Server locally
---------------------------

To run Axon Server locally, all you need to do is put the server JAR file in
the directory where you want it to live, and start it using:

    java -jar axonserver.jar

You will see that it creates a subdirectory data where it will store its
information. Open a browser to view information on the running Axon Server.

Configuring Axon Server
=======================

Axon Server uses sensible defaults for all of its settings, so it will actually
run fine without any further configuration. However, if you want to make some
changes, below are the most common options. You can change them using an
"axonserver.properties" file in the directory where you run Axon Server. For the
full list, see the Reference Guide. https://docs.axoniq.io/reference-guide/axon-server

* axoniq.axonserver.name
  This is the name Axon Server uses for itself. The default is to use the
  hostname.
* axoniq.axonserver.hostname
  This is the hostname clients will use to connect to the server. Note that
  an IP address can be used if the name cannot be resolved through DNS.
  The default value is the actual hostname reported by the OS.
* server.port
  This is the port where Axon Server will listen for HTTP requests,
  by default 8024.
* axoniq.axonserver.port
  This is the port where Axon Server will listen for gRPC requests,
  by default 8124.
* axoniq.axonserver.event.storage
  This setting determines where event messages are stored, so make sure there
  is enough diskspace here. Losing this data means losing your Events-sourced
  Aggregates' state! Conversely, if you want a quick way to start from scratch,
  here's where to clean.
* axoniq.axonserver.controldb-path
  This setting determines where the message hub stores its information.
  Losing this data will affect Axon Server's ability to determine which
  applications are connected, and what types of messages they are interested
  in.
* axoniq.axonserver.accesscontrol.enabled
  Setting this to true will require clients to pass a token.

The Axon Server HTTP server
===========================

Axon Server provides two servers; one serving HTTP requests, the other gRPC.
By default these use ports 8024 and 8124 respectively, but you can change
these in the settings as described above.

The HTTP server has in its root context a management Web GUI, a health
indicator is available at "/actuator/health", and the REST API at "/v1'. The
API's Swagger endpoint finally, is available at "/swagger-ui.html", and gives
the documentation on the REST API.
