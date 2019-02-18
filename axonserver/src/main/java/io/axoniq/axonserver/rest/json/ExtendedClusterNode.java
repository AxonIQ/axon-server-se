package io.axoniq.axonserver.rest.json;

import io.axoniq.axonserver.KeepNames;
import io.axoniq.axonserver.topology.AxonServerNode;

import java.util.Collection;

/**
 * @author Marc Gathier
 */
@KeepNames
public class ExtendedClusterNode implements AxonServerNode {

    private final AxonServerNode delegate;
    private boolean authentication;
    private boolean clustered;
    private boolean ssl;
    private boolean adminNode;

    public ExtendedClusterNode(AxonServerNode delegate) {
        this.delegate = delegate;
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


    @Override
    public String getHostName() {
        return delegate.getHostName();
    }

    @Override
    public Integer getGrpcPort() {
        return delegate.getGrpcPort();
    }

    @Override
    public String getInternalHostName() {
        return delegate.getInternalHostName();
    }

    @Override
    public Integer getGrpcInternalPort() {
        return delegate.getGrpcInternalPort();
    }

    @Override
    public Integer getHttpPort() {
        return delegate.getHttpPort();
    }

    @Override
    public String getName() {
        return delegate.getName();
    }

    public void setAdminNode(boolean adminNode) {
        this.adminNode = adminNode;
    }

    public boolean isAdminNode() {
        return adminNode;
    }

    @Override
    public Collection<String> getContextNames() {
        return delegate.getContextNames();
    }
}
