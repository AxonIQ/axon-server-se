This is the Axon Server Enterprise Edition, version 4.1.3

For information about the Axon Framework and Axon Server,
visit https://docs.axoniq.io.

Running Axon Server Enterprise edition
--------------------------------------

Axon Server Enterprise edition is expected to run in a cluster. If you want to start
using the Enterprise edition you will have to run an initialization command on the first
node in the cluster, and after that add other nodes to the first node.

To start the server on a specific node run the command:

    java -jar axonserver.jar

On the first node initialize the cluster using:

    java -jar axonserver-cli.jar init-cluster

On the other nodes, connect to the first node using:

    java -jar axonserver-cli.jar register-node -h <first-node-hostname>

For more information on setting up clusters and context check the reference guide at:

https://docs.axoniq.io/reference-guide/operations-guide/setting-up-axon-server

Once Axon Server is running you can view its configuration using the Axon Dashboard at http://<axonserver>:8024.

Changes in Axon Server 4.1.3
-------------------------------

- Improved recovery of cluster after disk failure

- Fix for updating configuration during install snapshot

- CLI commands now can be performed locally without token.

Changes in Axon Server 4.1.2
-------------------------------

- Improvements in replication to nodes that have been down for a long time

- Tracking event processor auto-loadbalancing fixes

- Status displayed for tracking event processors fixed when segments are running in different applications

- Tracking event processors are updated in separate thread

- Logging does not show application data anymore

- Fixed Axon Dashboard list of cluster nodes missing hostname and ports

- Changed some gRPC error codes returned to avoid clients to disconnect

- CLI commands init-cluster/register-node/add-node-to-context are now idempotent

- Register node returns faster (no longer waits for synchronization to new node)

- Reduced risk of re-election if a node restarts

- Fixed occasional NullPointerException when client connects to a newly registered Axon Server node

- Fixed incorrect leader returned in context API and multiple leaders warning in log


Changes in Axon Server 4.1.1
----------------------------

- Default controldb connection settings changed

- gRPC version update

- Register node no longer needs to be sent to leader of _admin context

- Merge tracking event processor not always available when it should

- Logging changes

- Fix for queries timeout

- Fix for replication with large messages

- Added axonserver-cli.jar to release package (axoniq-cli.jar is deprecated)


Changes in Axon Server 4.1
--------------------------

- Support for Split/Merge of tracking event processors through user interface

- Introduction of _admin context for all cluster management operations

- Updated process for getting started (see above)

- Replication of data and configuration between nodes in the cluster is now based on transaction log replication.
  You will see new files created on AxonServer nodes in the log directory (axoniq.axonserver.replication.log-storage-folder).
  Do not delete those files manually!

- Default setting for health endpoint (/actuator/heath) has changed to show more details.

- Change in TLS configuration for communication between AxonServer nodes (new property axoniq.axonserver.ssl.internal-trust-manager-file)


Migrating from Axon Server Enterprise edition 4.0
-------------------------------------------------

As the internal communication between Axon Server nodes has changed between versions 4.0 and 4.1 it is not possible to
perform a rolling update of the nodes.

To upgrade to 4.1 take the following steps:

1. Stop all applications connected to Axon Server

2. Stop all Axon Server nodes

3. Verify that the event files and snapshot files are equal on all nodes by calculating the md5 hash for all *.events and *.snapshot files.
   On Linux based systems you can use the command md5sum for this.

4. Unpack the new Axon Server version on the nodes

5. Check the axonserver.properties file, set property axoniq.axonserver.replication.log-storage-folder to a directory where the transaction log
   files for replication should be stored

6. Start all nodes, this will migrate the configuration data and create an _admin context will all nodes assigned to it.

7. Start all applications connected to Axon Server

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
* axoniq.axonserver.internal-port
  This is the port where Axon Server will listen for gRPC requests from other AxonServer nodes,
  by default 8224.
* axoniq.axonserver.event.storage
  This setting determines where event messages are stored, so make sure there
  is enough diskspace here. Losing this data means losing your Events-sourced
  Aggregates' state! Conversely, if you want a quick way to start from scratch,
  here's where to clean.
* axoniq.axonserver.snapshot.storage
  This setting determines where snapshot messages are stored, so make sure there
  is enough diskspace here. Losing this data means losing your Events-sourced
  Aggregates' state! Conversely, if you want a quick way to start from scratch,
  here's where to clean.
* axoniq.axonserver.controldb-path
  This setting determines where Axon Server stores its configuration information.
  Losing this data will affect Axon Server's ability to determine which
  applications are connected, and what types of messages they are interested
  in.
* axoniq.axonserver.replication.log-storage-folder
  This setting determines where the replication logfiles are stored.
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
