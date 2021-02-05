/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.rest.json;

import io.axoniq.axonserver.topology.AxonServerNode;

/**
 * Contains information on the current node, including the gRPC and HTTP ports, and Axon Server configuration (SSL, Authentication, Cluster).
 *
 * @author Marc Gathier
 */
public class NodeConfiguration {

    private final AxonServerNode delegate;
    private boolean authentication;
    private boolean clustered;
    private boolean ssl;
    private boolean adminNode;
    private boolean developmentMode;
    private Iterable<String> storageContextNames;
    private Iterable<String> contextNames;
    private boolean extensionsEnabled;

    public NodeConfiguration(AxonServerNode delegate) {
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

    /**
     * @return whether development mode is enabled
     */
    public boolean isDevelopmentMode() {
        return developmentMode;
    }

    /**
     * Setting to <code>true</code> enables features for development convenience (disabled by default)
     * @param developmentMode
     */
    public void setDevelopmentMode(boolean developmentMode) {
        this.developmentMode = developmentMode;
    }

    public String getHostName() {
        return delegate.getHostName();
    }

    public Integer getGrpcPort() {
        return delegate.getGrpcPort();
    }

    public String getInternalHostName() {
        return delegate.getInternalHostName();
    }

    public Integer getGrpcInternalPort() {
        return delegate.getGrpcInternalPort();
    }

    public Integer getHttpPort() {
        return delegate.getHttpPort();
    }

    public String getName() {
        return delegate.getName();
    }

    public void setAdminNode(boolean adminNode) {
        this.adminNode = adminNode;
    }

    public boolean isAdminNode() {
        return adminNode;
    }

    public Iterable<String> getContextNames() {
        return contextNames;
    }

    public void setStorageContextNames(Iterable<String> storageContextNames) {
        this.storageContextNames = storageContextNames;
    }

    public void setContextNames(Iterable<String> contextNames) {
        this.contextNames = contextNames;
    }

    public Iterable<String> getStorageContextNames() {
        return storageContextNames;
    }

    public void setExtensionsEnabled(boolean extensionsEnabled) {
        this.extensionsEnabled = extensionsEnabled;
    }

    public boolean getExtensionsEnabled() {
        return extensionsEnabled;
    }
}
