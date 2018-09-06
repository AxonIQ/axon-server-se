package io.axoniq.axonhub.rest;

import io.axoniq.axonhub.KeepNames;

/**
 * Author: marc
 */
@KeepNames
class ExtendedClusterNode extends ClusterNode{
    private boolean authentication;
    private boolean clustered;
    private boolean ssl;

    ExtendedClusterNode(String name, String hostName, String internalHostName, int internalGrpcPort, int grpcPort, int httpPort) {
        super(name, hostName, internalHostName, internalGrpcPort, grpcPort, httpPort);
    }

    public boolean isAuthentication() {
        return authentication;
    }

    public void setAuthentication(boolean authentication) {
        this.authentication = authentication;
    }

    public boolean isClustered() {
        return clustered;
    }

    public void setClustered(boolean clustered) {
        this.clustered = clustered;
    }

    public boolean isSsl() {
        return ssl;
    }

    public void setSsl(boolean ssl) {
        this.ssl = ssl;
    }
}
