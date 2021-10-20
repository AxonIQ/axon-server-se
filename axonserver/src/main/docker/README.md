<!-- Copyright (c) 2018-2020 by Axoniq B.V. - All rights reserved -->
# Axon Server

### Supported tags

* distroless-java 11 based (gcr.io/distroless/java:11):
  * 4.5.7, latest
  * 4.5.6
  * 4.5.5
  * 4.5.4.1
  * 4.5.3
  * 4.5.2
  * 4.5.1
  * 4.5
  * 4.4.12
  * 4.4.11
  * 4.4.10
  * 4.4.9
  * 4.4.8
  * 4.4.7
  * 4.4.6
  * 4.4.5
  * 4.4.4
  * 4.4.3
  * 4.4.2
  * 4.4.1
  * 4.4
  * 4.3.9
  * 4.3.8
  * 4.3.7
  * 4.3.6
  * 4.3.5
  * 4.3.4
  * 4.3.3
  * 4.3.2
  * 4.3.1
  * 4.3

* OpenJDK-11 based ([openjdk:11-jdk, aka openjdk:11.0.5-jdk-stretch](https://hub.docker.com/_/openjdk)):
  * 4.2.4-jdk11
  * 4.2.3-jdk11
  * 4.2.2-jdk11
  * 4.2-jdk11
  * 4.1.7-jdk11
  * 4.1.6-jdk11
  * 4.1.5-jdk11
  * 4.1.4-jdk11
  * 4.1.3-jdk11
  * 4.1.2-jdk11
  * 4.1.1-jdk11
  * 4.1-jdk11
  * 4.0.4-jdk11
  * 4.0.3-jdk11
* OpenJDK 8 based ([openjdk:8-jdk, aka openjdk:8u232-jdk-stretch](https://hub.docker.com/_/openjdk)):
  * 4.2.4
  * 4.2.3
  * 4.2.2
  * 4.2
  * 4.1.7
  * 4.1.6
  * 4.1.5
  * 4.1.4
  * 4.1.3
  * 4.1.2
  * 4.1.1
  * 4.1
  * 4.0.4
  * 4.0.3
  * 4.0.2
  * 4.0
### Quick Reference

* **License:**
  * Axon Server is provided under the AxonIQ Open Source License v1.0. (link to follow)

* **Where to get information:**

  * The [Axon Reference Guide](https://docs.axoniq.io/reference-guide/) is the definitive guide on the Axon Framework and Axon Server.
  * Visit [www.axoniq.io](https://www.axoniq.io) to find out about AxonIQ, the team behind the Axon Framework and Server.
  * Subscribe to the [AxonIQ Youtube channel](https://www.youtube.com/AxonIQ) to get the latest Webinars, announcements, and customer stories.
  * The latest version of the Giftcard App can be found [on GitHub](https://github.com/AxonIQ/giftcard-demo).
  * Docker images for Axon Server are pushed to [Docker Hub](https://hub.docker.com/u/axoniq).

* **Maintained by:**
  [The AxonIQ team](https://www.axoniq.io)

## Release Notes for version 4.5.7

* Fix: UI issues when running with another context root
* Fix: UI does not refresh the icons for event processor streams
* Fix: Balancing processors for a processing group containing special characters does not work from the UI
* Fix: Warning logged when a client closes an  event stream while it is reading from old segments
* Remove timing metrics for commands/queries for clients no longer connected

## Release Notes for version 4.5.6

* Fix: Memory leak in subscription query registrations

## Release Notes for version 4.5.5

* Fix: Improved error handling and feedback when uploading invalid plugins
* Fix: Increase default settings for spring.servlet.multipart.max-request-size and spring.servlet.multipart.max-file-size to 25MB

## Release Notes for version 4.5.4.1

* Fix: In case of timeout during query execution, AS sends a timeout error to the client before canceling the query.
* Fix: Close event store segment file when reading is complete

## Release Notes for version 4.5.3

* Fix: Reset event store with multiple segments
* Fix: Regression in loading aggregate events performance
* Fix: Handle queries with same request type but different response type
* New metrics added:
  - file.bloom.open: counts the number of bloom filter segments opened since start
  - file.bloom.close: counts the number of bloom filter segments closed since start
  - file.bloom.open: counts the number of bloom filter segments opened since start
  - file.segment.open: counts the number of event store segments opened since start
  - local.aggregate.segments: monitors the number of segments that were accessed for reading aggregate event requests

Notes:
- Default value for configuration property axoniq.axonserver.event.events-per-segment-prefetch is decreased from 50 to 10.

## Release Notes for version 4.5.2
* Improved performance for reading aggregates
* Improvements in shutdown process
* Reduced memory usage for in memory indexes
* Fix: Load balancing operations for processors should ignore stopped instances
* Fix: Stop reading events when query deadline expires

## Release Notes for version 4.5.1
* Configurable strategy for aggregate events stream sequence validation
* Fix UI check for updates

## Release Notes for version 4.5

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

## Release Notes for version 4.4.12
* Fix: Load balancing operations for processors should ignore stopped instances
* Fix: Stop reading events when query deadline expires

## Release Notes for version 4.4.11
* Configurable strategy for aggregate events stream sequence validation

## Release Notes for version 4.4.10
* Fix for subscription queries in case of missing query handler

## Release Notes for version 4.4.9
* Fix for concurrency issue in listing aggregates events during appending events for the same aggregate

## Release Notes for version 4.4.8
* New metric to monitor query response times per query handler

## Release Notes for version 4.4.7
* Improvement for subscription query: the initial result is now provided by a single instance per component
## Release Notes for version 4.4.6
* Fix for processor information showing information on disconnected applications
* Fix for issue with null expressions in ad-hoc queries
* Updated GRPC version to 1.34.0
* Added option to limit the number of commands/queries in progress

## Release Notes for version 4.4.5
* Improved reporting of errors while initializing the event store
* Fix for NullPointerException when event processor status was sent to Axon Server before the registration request
  was processed
* Improved handling of request processor status after an application disconnect

## Release Notes for version 4.4.4
* Improved QueryService logging
* Added preserve event store option to delete context CLI command
* Fixed stream completed by the server in case of inactivity
* Hide upload license panel in SE
* Reduced number of open index files
* Fix for GetTokenAt operation

## Release Notes for version 4.4.3
* Fix for connections not correctly registered
* Changed initialization sequence for event store to initialize completed segments first

## Release Notes for version 4.4.2
* Offload expensive data-writing operations to a separate thread pool
* Fix for reading aggregates with older snapshots

## Release Notes for version 4.4.1
* Reduced latency when Tracking live Events from a follower
* Improved handling of full queue to client
* Fix the refresh of the event processor status

## Release Notes for version 4.4
* Axon Server can now act as an event scheduler
* tag-based routing of commands and queries
* support for token store identifiers to identify which tracking event processors share a token store

## Release Notes for version 4.3.9
* Fixed stream completed by the server in case of inactivity

## Release Notes for version 4.3.8
* Complete the stream in case of exception during the load of events.
* Fix for connections not correctly registered

## Release Notes for version 4.3.7
* Fix race condition in queries and commands handlers unsubscription during reconnection

## Release Notes for version 4.3.6
* Fixed concurrency issue in subscribing/unsubscribing commands

## Release Notes for version 4.3.5
* Fixed logging in IndexManager

## Release Notes for version 4.3.4

* Reduced risk for contention when opening an index file
* Offload expensive data-fetching operations to a separate thread pool
* Option to configure the way that index files are opened (memory-mapped or file channel-based)
* Limit the number of commands/queries held in Axon Server waiting for the handlers to be ready to handle them, to avoid
  out of memory errors on Axon Server

## Release Notes for version 4.3.3

* Fix for validation error starting up when there are multiple snapshot files (Standard Edition only)

## Release Notes for version 4.3.2

* Fix for tracking event processor updates to websocket causing high CPU load in specific situation
* Reduced warnings in log file on clients disconnecting
* Fix for concurrency issue in sending heartbeat while client connects/disconnects

## Release Notes for version 4.3.1

* Updated usage output in CLI
* Updated gRPC/Netty versions
* Prevent errors in log (sending ad-hoc result to client that has gone, sending heartbeat to client that has gone)

## Release Notes for version 4.3

* changed docker base image to gcr.io/distroless/java:11. The new docker image uses 2 volumes, /eventdata as the base directory for the event store and /data as the base directory for all other files created by Axon Server).
* metrics changed to include tags
* separate log writer for audit log

## Release Notes for version 4.2.4

* Improved support for running management server on separate port

## Release Notes for version 4.2.3

* Fix for pending queries with lost connection

## Release Notes for version 4.2.2

* Cleaned-up logging
* Fix for specific error while reading aggregate
* Optional heartbeat between Axon Server and Axon Framework clients

## Release Notes for version 4.2

* Development mode with delete all events operation added
* Blacklisting event types for applications that cannot handle these events
* Expose tracking event processor position and status

## Release Notes for version 4.1.7

* Fix for saving aggregates failing with error "Invalid Sequence number" in specific circumstances

## Release Notes for version 4.1.6

* Added operation to set cached version numbers for aggregates

## Release Notes for version 4.1.5

* Fix for authorization path mapping and improvements for rest access control

* Improvements in release procedure for docker images

* Fix for subscription query memory leak

* Improvements in error reporting in case of disconnected applications

* Improvements in detection of insufficient disk space

## Release Notes for version 4.1.4

* Fix for appendEvent with no events in stream

## Release Notes for version 4.1.3

* CLI commands now can be performed locally without token.

## Release Notes for version 4.1.2

* Status displayed for tracking event processors fixed when segments are running in different applications
* Tracking event processors are updated in separate thread
* Logging does not show application data anymore
* Changed some gRPC error codes returned to avoid clients to disconnect when no command handler found for a command

## Release Notes for version 4.1.1

* Sources now available in public GitHub repository
* Merge tracking event processor not always available when it should
* Logging changes
* GRPC version update

## Release Notes for version 4.1

* Added split/merge functionality for tracking event processors

## Release Notes for version 4.0.4

* Fixed a bug related to storing events with large gaps in sequence numbers.

## Release Notes for version 4.0.3

* Support for Java versions 10 and 11
* Actuator endpoints no longer require AxonIQ-Access-Token when access control enabled

## Release Notes for version 4.0.2

* Performance improvements for event replay.
* Changes in the event filename format, to match AxonDB filenames.

## Running Axon Server

To run Axon Server in Docker you can use the image provided on Docker Hub:

```
$ docker run -d --name my-axon-server -p 8024:8024 -p 8124:8124 axoniq/axonserver
...some container id...
```

For more information on how to run Axon Server in a Docker/Kubernetes environment check the [reference guide](https://docs.axoniq.io/reference-guide/axon-server/installation/docker-k8s/axon-server-se).



## Configuring Axon Server

Axon Server uses sensible defaults for all of its settings, so it will actually run fine without any further configuration. However, if you want to make some changes, below are the most common options.

### Environment variables for customizing the Docker image of Axon Server

* `JAVA_TOOL_OPTIONS` (prior to 4.3 `JAVA_OPTS`)

  This is used to pass JVM options. Default is to use it for heap size setting, with "`-Xmx512m`".
* `AXONIQ_AXONSERVER_NAME`

  This is the name the Axon Server uses for itself.
* `AXONIQ_AXONSERVER_HOSTNAME`

  This is the hostname Axon Server communicates to the client as its contact point. Default is "`localhost`", because Docker generates a random name that is not resolvable outside of the container.
* `AXONIQ_AXONSERVER_DOMAIN`

  This is the domain Axon Server can suffix the hostname with.
* `SERVER_PORT`

  This is the port Axon Server uses for its Web GUI and REST API.
* `AXONIQ_AXONSERVER_PORT`

  This is the gRPC port used by clients to exchange data with the server.
* `AXONIQ_AXONSERVER_ACCESSCONTROL_ENABLED` and `AXONIQ_AXONSERVER_ACCESSCONTROL_TOKEN`

  Setting enabled to true will enable access control. The clients need to pass the specified token with each request.
* `AXONIQ_AXONSERVER_EVENT_STORAGE`

  This is the directory used for storing the Events.
* `AXONIQ_AXONSERVER_SNAPSHOT_STORAGE`

  This is the directory used for storing the replication log.
* `AXONIQ_AXONSERVER_CONTROLDB-PATH`

  The directory where Axon Server keeps its internal DB.


### The Axon Server HTTP server

Axon Server provides two servers; one serving HTTP requests, the other gRPC. By default, these use ports 8024 and 8124 respectively, but you can change these in the settings.

The HTTP server has in its root context a management Web GUI, a health indicator is available at `/actuator/health`, and the REST API at `/v1`. The API's Swagger endpoint finally, is available at `/swagger-ui.html`, and gives the documentation on the REST API.
