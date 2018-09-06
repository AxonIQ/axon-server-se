# Messaging Platform Cluster

Messaging platform is clustered in an active-active configuration. Each node can be used to connect applications to, both as a subscriber and as a consumer.

When an application connects to one of the nodes (using the ClusterService), this node determines the best node for the application to use.
The response from the ClusterService gives the information about the preferred node.

The AxonIQ messaging client will connect to the preferred node, other clients may choose not to do this.

When a node is disconnected from the cluster, the connections to the applications are closed. The applications will have to reconnect to another node.
The AxonIQ messaging client handles this automatically.

![Cluster Overview](/images/Cluster.png)

When an application is connected to one of the nodes, and subscribes to commands and/or queries,
the node forwards these subscriptions to all connected messaging platform nodes.

## Node assignment

The messaging platform returns the node to connect to on the initial connect request.
When there are multiple instances of the same component (application) running, it keeps
the connections on the same node.  

## Set-up

Setting up a complete cluster with authentication and SSL requires the following steps:

1. Get axoniq.licence file with at lease Standard edition license and add to installation
   package.

1. Create certificates for the server
 
  - Create a private key
```bash
openssl genrsa -out axoniq.key 2048
openssl pkcs8 -topk8 -nocrypt -in axoniq.key -out axoniq.pem
```
  - Create a certificate for the domain level
```bash
openssl req -new -out axoniq.csr -key axoniq.key
```  
```bash
openssl x509 -req -days 1825 -in axoniq.csr -signkey axoniq.key -out axoniq.crt
```  
  - add certificates to installation package (axoniq.pem and axoniq.crt)

1. Create the correct axonhub.properties file

  - axoniq.axonhub.cluster.enabled=true
  - axoniq.axonhub.ssl.enabled=true
  - axoniq.axonhub.ssl.certChainFile=axoniq.crt
  - axoniq.axonhub.ssl.privateKeyFile=axoniq.pem
  - axoniq.axonhub.accesscontrol.enabled=true
  - axoniq.axonhub.accesscontrol.internalToken=123456
  
1. Start the nodes in the cluster

1. When all nodes are active, register an application
```
  ./cli.jar register-application -S http://localhost:8024 -a admin -r ADMIN
```
  - this outputs a key to be used by client applications, to automatically use this key
  in the client applications store it in a file in a shared location to be read by clients on startup.
  
1. register an admin user
```
./cli.jar register-user -S http://localhost:8023 -u marc -r ADMIN
```  

1. For each application, add the access token to the application.properties

When using Docker use openjdk:8-jre as base image, as the alpine image does not have support
for netty-tcnative-boringssl. 

### Using Docker

Start with the normal docker image for axonhub (which is built by the maven build profile docker).

Derive a new image containing the updated axonhub.properties, a license file and the 
files for SSL.

```dockerfile
FROM eu.gcr.io/kubernetes-marc-207206/axonhub:1.2-SNAPSHOT
EXPOSE 8024 8124 8224

RUN apt-get update
RUN apt-get install dos2unix

ADD axoniq.pem .
ADD axoniq.license .
ADD axoniq.crt .
ADD axonhub.properties .
ADD startup.sh .
ADD register.sh .
ADD register_app_user.sh .

RUN dos2unix *.properties *.sh
RUN chmod +x *.sh *.jar
ENTRYPOINT ["/startup.sh"]
```

The docker image contains a new version of the startup.sh and register.sh scripts. The updated register script uses the base URL to check 
if the node is active as the health URL never returns successfully when access control is enabled as it expects a token.
The updated startup script starts the register_app_user.sh script on the first node.

The register_app_user script creates an application and stores the token in the file /app/admin.token, this can then be used by applications 
to 

Build the docker image
```bash
docker build -t axonhub:1.2-cluster .
```

Docker preparation

Setup a network axoniq
```bash
docker network create axoniq
```

Setup a shared volume, that will be used to save application access tokens.

```bash
docker volume create axoniq.volume
```

Create the containers
```bash
docker run -dit --name axonhub-0 -h axonhub-0.axoniq --network axoniq --network-alias axonhub --mount source=axoniq.volume,target=/app axonhub:1.2-cluster
docker run -dit --name axonhub-1 -h axonhub-1.axoniq --network axoniq --network-alias axonhub --mount source=axoniq.volume,target=/app axonhub:1.2-cluster
docker run -dit --name axonhub-2 -h axonhub-2.axoniq --network axoniq --network-alias axonhub --mount source=axoniq.volume,target=/app axonhub:1.2-cluster
```


Running the sample application
```bash
docker run -dit --name sample --network axoniq --mount source=axoniq.volume,target=/app sample:1.0
```
### Using Kubernetes 

