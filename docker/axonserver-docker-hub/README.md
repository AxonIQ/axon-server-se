<!-- Copyright (c) 2018 by Axoniq B.V. - All rights reserved -->
# Axon Server

### Supported tags

* OpenJDK 8 based ([openjdk:8-jdk, aka openjdk:8u181-jdk-stretch](https://hub.docker.com/_/openjdk)):
    * 4.1.2, latest ([Dockerfile](https://github.com/AxonIQ/axon-server-dockerfile/blob/master/src/main/docker/Dockerfile))
    * 4.1.1
    * 4.1
    * 4.0.4
    * 4.0.3
    * 4.0.2
    * 4.0
* OpenJDK-11 based ([openjdk:11-jdk, aka openjdk:11.0.1-jdk-stretch](https://hub.docker.com/_/openjdk)):
    * 4.1.2-jdk11
    * 4.1.1-jdk11
    * 4.1-jdk11
    * 4.0.4-jdk11
    * 4.0.3-jdk11

### Quick Reference

* **License:**
    * The Axon Server Dockerfile, asociated build scripts and documentation, are licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at:

        http://www.apache.org/licenses/LICENSE-2.0

      Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
    * Axon Server is provided under the AxonIQ Open Source License v1.0. (link to follow)

* **Where to get information:**

    * This README, the latest Dockerfile and associated scripts can be found [on GitHub](https://github.com/AxonIQ/axon-server-dockerfile.git).
    * The [Axon Reference Guide](https://docs.axoniq.io/reference-guide/) is definitive guide on the Axon Framework and Axon Server.
    * Visit [www.axoniq.io](https://www.axoniq.io) to find out about AxonIQ, the team behind the Axon Framework and Server.
    * Subscribe to the [AxonIQ Youtube channel](https://www.youtube.com/AxonIQ) to get the latest Webinars, announcements, and customer stories.
    * The latest version of the Giftcard App can be found [on GitHub](https://github.com/AxonIQ/giftcard-demo).
    * Docker images for Axon Server are pushed to [Docker Hub](https://hub.docker.com/u/axoniq).

* **Maintained by:**
    [The AxonIQ team](https://www.axoniq.io)

The text below is an extract (with small textual adjustments) of the README in the [GiftCard demo repo on GitHub](https://github.com/AxonIQ/giftcard-demo).

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

*WARNING* This is not a supported image for production purposes. Please use with caution.

If you want to run the clients in Docker containers as well, and are not using something like Kubernetes, use the "`--hostname`" option of the `docker` command to set a useful name like "axonserver", and pass the `AXONSERVER_HOSTNAME` environment variable to adjust the properties accordingly:
```
$ docker run -d --name my-axon-server -p 8024:8024 -p 8124:8124 --hostname axonserver -e AXONSERVER_HOSTNAME=axonserver axoniq/axonserver
```

When you start the client containers, you can now use "`--link axonserver`" to provide them with the correct DNS entry. The Axon Server-connector looks at the "`axon.axonserver.servers`" property to determine where Axon Server lives, so don't forget to set it to "`axonserver`".

### Running Axon Server in Kubernetes and Mini-Kube

*WARNING*: Although you can get a pretty functional cluster running locally using minikube, you can run into trouble when you want to let it serve clients outside of the cluster. Minikube can provide access to HTTP servers running in the cluster, for other protocols you have to run a special protocol-agnostic proxy like you can with "`kubectl port-forward` _&lt;pod-name&gt;_ _&lt;port-number&gt;_". For non-development scenarios, we don't recommend using minikube.

Assuming you have access to a working cluster, either using minikube or with a 'real' setup, make sure you have the Docker environment variables set correctly to use it. With minikube this is done using the "`minikube docker-env`" command, with the Google Cloud SDK use "`gcloud auth configure-docker`".

Deployment requires the use of a YAML descriptor, an example of which can be found in the "`kubernetes`" directory of the GitHub repository. To run it, use the following commands in a separate window:

```
$ kubectl apply -f kubernetes/axonserver.yaml
statefulset.apps "axonserver" created
service "axonserver-gui" created
service "axonserver" created
$ kubectl port-forward axonserver-0 8124
Forwarding from 127.0.0.1:8124 -> 8124
Forwarding from [::1]:8124 -> 8124
```

You can now run the Giftcard app, which will connect throught the proxied gRPC port. To see the Axon Server Web GUI, use "`minikube service --url axonserver-gui`" to obtain the URL for your browser. Actually, if you leave out the "`--url`", minikube will open the the GUI in your default browser for you.

To clean up the deployment, use:

```
$ kubectl delete sts axonserver
statefulset.apps "axonserver" deleted
$ kubectl delete svc axonserver
service "axonserver" deleted
$ kubectl delete svc axonserver-gui
service "axonserver-gui" deleted
```

## Configuring Axon Server

Axon Server uses sensible defaults for all of its settings, so it will actually run fine without any further configuration. However, if you want to make some changes, below are the most common options.

### Environment variables for customizing the Docker image of Axon Server

The `axoniq/axonserver` image can be customized at start by using one of the following environment variables. If no default is mentioned, leaving the environement variable unspecified will not add a line to the properties file.

* `JAVA_OPTS`

    This is used to pass JVM options. Default is to use it for heap size setting, with "`-Xmx512m`".
* `AXONSERVER_NAME`

    This is the name the Axon Server uses for itself.
* `AXONSERVER_HOSTNAME`

    This is the hostname Axon Server communicates to the client as its contact point. Default is "`localhost`", because Docker generates a random name that is not resolvable outside of the container.
* `AXONSERVER_DOMAIN`

    This is the domain Axon Server can suffix the hostname with.
* `AXONSERVER_HTTP_PORT`

    This is the port Axon Server uses for its Web GUI and REST API.
* `AXONSERVER_GRPC_PORT`

    This is the gRPC port used by clients to exchange data with the server.
* `AXONSERVER_TOKEN`

    Setting this will enable access control, which means the clients need to pass this token with each request.
* `AXONSERVER_EVENTSTORE`

    This is the directory used for storing the Events.
* `AXONSERVER_LOGSTORE`

    This is the directory used for storing the replication log.
* `AXONSERVER_CONTROLDB`

    This is where Axon Server stores information of clients and what types of messages they are interested in.

### Axon Server configuration

There are a number of things you can finetune in the server configuration. You can do this using an "`axonserver.properties`" file. All settings have sensible defaults.

* `axoniq.axonserver.name`

    This is the name Axon Server uses for itself. The default is to use the hostname.
* `axoniq.axonserver.hostname`

    This is the hostname clients will use to connect to the server. Note that an IP address can be used if the name cannot be resolved through DNS. The default value is the actual hostname reported by the OS.
* `server.port`

    This is the port where Axon Server will listen for HTTP requests, by default `8024`.
* `axoniq.axonserver.port`

    This is the port where Axon Server will listen for gRPC requests, by default `8124`.
* `axoniq.axonserver.event.storage`

    This setting determines where event messages are stored, so make sure there is enough diskspace here. Losing this data means losing your Events-sourced Aggregates' state! Conversely, if you want a quick way to start from scratch, here's where to clean.
* `axoniq.axonserver.controldb-path`

    This setting determines where the message hub stores its information. Losing this data will affect Axon Server's ability to determine which applications are connected, and what types of messages they are interested in.
* `axoniq.axonserver.accesscontrol.enabled`

    Setting this to `true` will require clients to pass a token.
* `axoniq.axonserver.accesscontrol.token`

    This is the token used for access control.

### The Axon Server HTTP server

Axon Server provides two servers; one serving HTTP requests, the other gRPC. By default these use ports 8024 and 8124 respectively, but you can change these in the settings.

The HTTP server has in its root context a management Web GUI, a health indicator is available at `/actuator/health`, and the REST API at `/v1`. The API's Swagger endpoint finally, is available at `/swagger-ui.html`, and gives the documentation on the REST API.
