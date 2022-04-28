This is the Axon Server Standard Edition, version 4.5

For information about the Axon Framework and Axon Server,
visit https://docs.axoniq.io.

Release Notes for version 4.5.12
--------------------------------
* Deprecated "/v1/backup/filenames" endpoint, use new endpoint /v1/backup/eventstore instead. The new endpoint returns all files
  to back up, given a last closed segment number, and it also returns the currently last closed segment.

Release Notes for version 4.5.11
--------------------------------
* Updated Spring Boot version to 2.5.12 to fix CVE-2022-22965

Release Notes for version 4.5.10
--------------------------------
* Updated gRPC version from 1.42.0 to 1.42.2 to avoid CVE-2021-22569

Release Notes for version 4.5.9
-------------------------------
* Updated gRPC and Netty versions
* Improved logging on client application disconnects
* Fix: missing/double icons on plugin page

Release Notes for version 4.5.8
-------------------------------
* Update Felix to version 7.0.1 to support java 17
* Update JQuery to version 3.6.0
* Fix: incorrect login url when AS is invoked behind a reverse proxy
* Fix: NullPointerException in health check

Release Notes for version 4.5.7
-------------------------------
* Fix: UI issues when running with another context root
* Fix: UI does not refresh the icons for event processor streams
* Fix: Balancing processors for a processing group containing special characters does not work from the UI
* Fix: Warning logged when a client closes an  event stream while it is reading from old segments
* Remove timing metrics for commands/queries for clients no longer connected

Release Notes for version 4.5.6
-------------------------------
* Fix: Memory leak in subscription query registrations

Release Notes for version 4.5.5
-------------------------------
* Fix: Improved error handling and feedback when uploading invalid plugins
* Fix: Increase default settings for spring.servlet.multipart.max-request-size and spring.servlet.multipart.max-file-size to 25MB

Release Notes for version 4.5.4.1
---------------------------------
* Fix: In case of timeout during query execution, AS sends a timeout error to the client before canceling the query.
* Fix: Close event store segment file when reading is complete

Release Notes for version 4.5.3
-------------------------------
* Fix: Reset event store with multiple segments
* Fix: Regression in loading aggregate events performance
* Fix: Handle queries with same request type but different response type
* New metrics added:
  - file.bloom.open: counts the number of bloom filter segments opened since start
  - file.bloom.close: counts the number of bloom filter segments closed since start
  - file.segment.open: counts the number of event store segments opened since start
  - local.aggregate.segments: monitors the number of segments that were accessed for reading aggregate event requests

Notes:
 - Default value for configuration property axoniq.axonserver.event.events-per-segment-prefetch is decreased from 50 to 10.

Release Notes for version 4.5.2
-------------------------------
* Improved performance for reading aggregates
* Improvements in shutdown process
* Reduced memory usage for in memory indexes
* Fix: Load balancing operations for processors should ignore stopped instances
* Fix: Stop reading events when query deadline expires

Release Notes for version 4.5.1
-----------------------------
* Configurable strategy for aggregate events stream sequence validation
* Fix UI check for updates

Release Notes for version 4.5
-----------------------------

New features:
- Support for customer defined plugins to add custom actions to adding/reading events and snapshots and executing commands and (subscription queries)
  For more information see https://docs.axoniq.io/reference-guide/axon-server/administration/plugins.
- Search snapshots in Axon Dashboard

Enhancements:
- Flow control for reading aggregates
- Logging of illegal access to Axon Server gRPC services
- Improved monitoring of available disk space (see https://docs.axoniq.io/reference-guide/axon-server/administration/monitoring/actuator-endpoints)
- List of used 3rd party libraries available from Dashboard
- Axon Dashboard checks for Axon Server version updates

Dependency updates:
- updated gRPC and Netty versions
- updated Spring Boot version
- updated Swagger version

Bug fixes:
- Read aggregate snapshots from closed segments fixed

Notes:
- Due to the update of the Spring Boot version there are some minor changes to the output of the /actuator/health endpoint.
  This endpoint now uses the element "components" instead of "details" to output the health per subcategory.

- The swagger endpoint has changed from /swagger-ui.html to /swagger-ui/.

- The default setting for "show-details" for the /actuator/health endpoint has changed from "never" to "always". To hide the
  details from unauthenticated users, set the property "management.endpoint.health.show-details" to "when-authorized".

- For the Docker image, plugins are stored in the /data/plugins directory.

Release Notes for version 4.4.13
--------------------------------
* Change in internal event handling required for Axon Server Enterprise Edition

Release Notes for version 4.4.12
-------------------------------
* Fix: Load balancing operations for processors should ignore stopped instances
* Fix: Stop reading events when query deadline expires

Release Notes for version 4.4.11
-------------------------------
* Configurable strategy for aggregate events stream sequence validation

Release Notes for version 4.4.10
-------------------------------
* Fix for subscription queries in case of missing query handler

Release Notes for version 4.4.10
--------------------------------
* Fix for subscription queries in case of missing query handler

Release Notes for version 4.4.9
-------------------------------
* Fix for concurrency issue in listing aggregates events during appending events for the same aggregate

Release Notes for version 4.4.8
-------------------------------
* New metric to monitor query response times per query handler

Release Notes for version 4.4.7
-------------------------------
* Improvement for subscription query: initial result are now provided by a single instance per component

Release Notes for version 4.4.6
-------------------------------
* Fix for processor information showing information on disconnected applications
* Fix for issue with null expressions in ad-hoc queries
* Updated GRPC version to 1.34.0
* Added option to limit the number of commands/queries in progress

Release Notes for version 4.4.5
-------------------------------
* Improved reporting of errors while initializing the event store
* Fix for NullPointerException when event processor status was sent to Axon Server before registration request
  was processed
* Improved handling of request processor status after an application disconnect

Release Notes for version 4.4.4
-------------------------------
* Improved QueryService logging
* Added preserve event store option to delete context CLI command
* Fixed stream completed by the server in case of inactivity
* Hide upload license panel in SE
* Reduced number of open index files
* Fix for GetTokenAt operation

Release Notes for version 4.4.3
-------------------------------
* Fix for connections not correctly registered
* Changed initialization sequence for event store to initialize completed segments first

Release Notes for version 4.4.2
-------------------------------
* Offload expensive data-writing operations to separate thread pool
* Fix for reading aggregates with older snapshots

Release Notes for version 4.4.1
-------------------------------
* Reduced latency when Tracking live Events from a follower
* Improved handling of full queue to client
* Fix the refresh of the event processor status

Release Notes for version 4.4
-----------------------------
* Axon Server can now act as an event scheduler
* tag-based routing of commands and queries
* support fom token store identifiers to identify which tracking event processors share a token store

Release Notes for version 4.3.9
-------------------------------
* Fixed stream completed by the server in case of inactivity

Release Notes for version 4.3.8
-------------------------------
* Complete the stream in case of exception during the load of events.
* Fix for connections not correctly registered

Release Notes for version 4.3.7
-------------------------------
* Fix race condition in queries and commands handlers unsubscription during reconnection

Release Notes for version 4.3.6
-------------------------------
* Fixed concurrency issue in subscribing/unsubscribing commands

Release Notes for version 4.3.5
-------------------------------

* Fixed logging in IndexManager

Release Notes for version 4.3.4
-------------------------------

* Reduced risk for contention when opening an index file
* Offload expensive data-fetching operations to separate thread pool
* Option to configure the way that index files are opened (memory mapped or file channel based)
* Limit the amount of commands/queries held in Axon Server waiting for the handlers to be ready to handle them, to avoid
  out of memory errors on Axon Server

Release Notes for version 4.3.3
-------------------------------

* Fix for validation error starting up when there are multiple snapshot files (Standard Edition only)

Release Notes for version 4.3.2
-------------------------------

* Fix for tracking event processor updates to websocket causing high CPU load in specific situation
* Reduced warnings in log file on clients disconnecting
* Fix for concurrency issue in sending heartbeat while client connects/disconnects

Release Notes for version 4.3.1
-------------------------------

* Updated usage output in CLI
* Updated gRPC/Netty versions
* Prevent errors in log (sending ad-hoc result to client that has gone, sending heartbeat to client that has gone)

Release Notes for version 4.3
-------------------------------

* Separate audit log for configuration changes
* Changed metrics to use common names and tags
* Changed docker image (base image changed and 2 volumes added)

Release Notes for version 4.2.4
-------------------------------

* Improved support for running management server on separate port

Release Notes for version 4.2.3
-------------------------------

* Fix for pending queries with lost connection

Release Notes for version 4.2.2
-------------------------------

* Added instruction acknowledgements
* Client applications heartbeat support
* Cleaned-up logging
* Fix for specific error while reading aggregate
* Optional heartbeat between Axon Server and Axon Framework clients

Release Notes for version 4.2.1
-------------------------------

* Fixes required for enterprise edition only

Release Notes for version 4.2
-------------------------------

* Development mode with delete all events operation added
* Blacklisting event types for applications that cannot handle these events
* Expose tracking event processor position and status

Release Notes for version 4.1.7
-------------------------------

* Use info endpoint to retrieve version number and product name
* Reset reserved sequence numbers for aggregate when storing the event failed

Release Notes for version 4.1.6
-------------------------------

* Added operation to set cached version numbers for aggregates

Release Notes for version 4.1.5
-------------------------------

* Fix for authorization path mapping and improvements for rest access control
* Improvements in release procedure for docker images
* Fix for subscription query memory leak
* Improvements in error reporting in case of disconnected applications
* Improvements in detection of insufficient disk space

Release Notes for version 4.1.4
-------------------------------
* Fix for appendEvent with no events in stream

Release Notes for version 4.1.3
-------------------------------
* CLI commands now can be performed locally without token.

Release Notes for version 4.1.2
-------------------------------

* Status displayed for tracking event processors fixed when segments are running in different applications
* Tracking event processors are updated in separate thread
* Logging does not show application data anymore
* Changed some gRPC error codes returned to avoid clients to disconnect when no command handler found for a command

Release Notes for version 4.1.1
-------------------------------

* Sources now available in public GitHub repository
* Merge tracking event processor not always available when it should
* Logging changes
* GRPC version update

Release Notes for version 4.1
-------------------------------

* Added split/merge functionality for tracking event processors

Release Notes for version 4.0.4
-------------------------------

* Fix for check on sequence numbers with large gap

Release Notes for version 4.0.3
-------------------------------

* Support for Java versions 10 and 11
* Actuator endpoints no longer require AxonIQ-Access-Token when access control enabled

Release Notes for version 4.0.2
-------------------------------

* Performance improvements for event replay.
* Changes in the event filename format to match AxonDB filenames

Running Axon Server
===================

By default the Axon Framework is configured to expect a running Axon Server
instance, and it will complain if the server is not found. To run Axon Server,
you'll need a Java runtime (JRE versions 8 through 11 are currently supported).
A copy of the server JAR file has been provided in the demo package.
You can run it locally, in a Docker container (including Kubernetes or even Minikube),
or on a separate server.

Running Axon Server locally
---------------------------

To run Axon Server locally, all you need to do is put the server JAR file in
the directory where you want it to live, and start it using:

    java -jar axonserver.jar

You will see that it creates a subdirectory data where it will store its
information. Open a browser to with URL http://localhost:8024 to view the dashboard.

Running Axon Server in a Docker container
-----------------------------------------

To run Axon Server in Docker you can use the image provided on Docker Hub:

    $ docker run -d --name my-axon-server -p 8024:8024 -p 8124:8124 axoniq/axonserver
    ...some container id...
    $

WARNING:
    This is not a supported image for production purposes. Please use with caution.

If you want to run the clients in Docker containers, and are not using something
like Kubernetes, use the "--hostname" option of the docker command to set a useful
name like "axonserver":

    $ docker run -d --name my-axon-server -p 8024:8024 -p 8124:8124 --hostname axonserver axoniq/axonserver

When you start the client containers, you can now use "--link axonserver" to provide
them with the correct DNS entry. The Axon Server-connector looks at the
"axon.axonserver.servers" property to determine where Axon Server lives, so don't
forget to set that to "axonserver" for your apps.

Running Axon Server in Kubernetes and Minikube
-----------------------------------------------

WARNING:
    Although you can get a pretty functional cluster running locally using Minikube,
    you can run into trouble when you want to let it serve clients outside of the
    cluster. Minikube can provide access to HTTP servers running in the cluster,
    for other protocols you have to run a special protocol-agnostic proxy like you
    can with "kubectl port-forward <pod-name> <port-number>".
    For non-development scenarios, we don't recommend using Minikube.

Deployment requires the use of a YAML descriptor, an working example of which
can be found in the "kubernetes" directory. To run it, use the following commands
in a separate window:

    $ kubectl apply -f kubernetes/axonserver.yaml
    statefulset.apps "axonserver" created
    service "axonserver-gui" created
    service "axonserver" created
    $ kubectl port-forward axonserver-0 8124
    Forwarding from 127.0.0.1:8124 -> 8124
    Forwarding from [::1]:8124 -> 8124


You can now run your app, which will connect throught the proxied gRPC port. To see
the Axon Server Web GUI, use "minikube service --url axonserver-gui" to obtain the
URL for your browser. Actually, if you leave out the "--url", minikube will open
the the GUI in your default browser for you.

To clean up the deployment, use:

    $ kubectl delete sts axonserver
    statefulset.apps "axonserver" deleted
    $ kubectl delete svc axonserver
    service "axonserver" deleted
    $ kubectl delete svc axonserver-gui
    service "axonserver-gui" deleted

Use "axonserver" (as that is the name of the Kubernetes service) for your clients
if you're going to deploy them in the cluster, which is what you'ld probably want.
Running the client outside the cluster, with Axon Server inside, entails extra
work to enable and secure this, and is definitely beyond the scope of this example.

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
* axoniq.axonserver.snapshot.storage
  This setting determines where aggregate snapshots are stored.
* axoniq.axonserver.controldb-path
  This setting determines where the message hub stores its information.
  Losing this data will affect Axon Server's ability to determine which
  applications are connected, and what types of messages they are interested
  in.
* axoniq.axonserver.accesscontrol.enabled
  Setting this to true will require clients to pass a token.
* axoniq.axonserver.accesscontrol.token
  This is the token used for access control.

The Axon Server HTTP server
===========================

Axon Server provides two servers; one serving HTTP requests, the other gRPC.
By default these use ports 8024 and 8124 respectively, but you can change
these in the settings as described above.

The HTTP server has in its root context a management Web GUI, a health
indicator is available at "/actuator/health", and the REST API at "/v1'. The
API's Swagger endpoint finally, is available at "/swagger-ui/", and gives
the documentation on the REST API.
