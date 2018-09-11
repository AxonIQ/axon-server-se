package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.KeepNames;
import io.axoniq.axonhub.internal.grpc.NodeInfo;

/**
 * Author: marc
 */
@KeepNames
class ClusterNode implements Comparable<ClusterNode> {
    private String hostName;
    private String internalHostName;
    private int internalGrpcPort;
    private int grpcPort;
    private int httpPort;
    private boolean connected;
    private boolean master;
    private String name;

    public ClusterNode() {

    }

    ClusterNode(String name, String hostName, String internalHostName, int internalGrpcPort, int grpcPort, int httpPort) {
        this.hostName = hostName;
        this.internalHostName = internalHostName;
        this.internalGrpcPort = internalGrpcPort;
        this.grpcPort = grpcPort;
        this.httpPort = httpPort;
        this.name = name;
    }

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public int getGrpcPort() {
        return grpcPort;
    }

    public void setGrpcPort(int grpcPort) {
        this.grpcPort = grpcPort;
    }

    public int getHttpPort() {
        return httpPort;
    }

    public void setHttpPort(int httpPort) {
        this.httpPort = httpPort;
    }

    public String getInternalHostName() {
        return internalHostName;
    }

    public void setInternalHostName(String internalHostName) {
        this.internalHostName = internalHostName;
    }

    public int getInternalGrpcPort() {
        return internalGrpcPort;
    }

    public void setInternalGrpcPort(int internalGrpcPort) {
        this.internalGrpcPort = internalGrpcPort;
    }

    public boolean isConnected() {
        return connected;
    }

    public ClusterNode setConnected(boolean connected) {
        this.connected = connected;
        return this;
    }

    public boolean isMaster() {
        return master;
    }

    public void setMaster(boolean master) {
        this.master = master;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public int compareTo( ClusterNode o) {
        return name.compareTo(o.name);
    }

    public NodeInfo asNodeInfo() {
        return NodeInfo.newBuilder().setNodeName(name)
                .setInternalHostName(internalHostName)
                .setHttpPort(httpPort)
                .setHostName(hostName)
                .setGrpcPort(grpcPort)
                .setGrpcInternalPort(internalGrpcPort)
                .setVersion(0)
                .build();
    }
}
