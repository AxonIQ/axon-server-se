/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.cli.json;

/**
 * The cluster node representation used within AxonServer Rest API calls.
 *
 * @author Marc Gathier
 */
public class ClusterNode {
    private String name;

    private String hostName;
    private String internalHostName;
    private Integer grpcPort;
    private Integer internalGrpcPort;
    private Integer httpPort;
    private boolean connected;
    private boolean master;

    private String context;
    private Boolean noContexts;

    public ClusterNode(String internalHostName, Integer internalGrpcPort) {
        this.internalHostName = internalHostName;
        this.internalGrpcPort = internalGrpcPort;
    }

    public ClusterNode() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public String getInternalHostName() {
        return internalHostName;
    }

    public void setInternalHostName(String internalHostName) {
        this.internalHostName = internalHostName;
    }

    public Integer getGrpcPort() {
        return grpcPort;
    }

    public void setGrpcPort(Integer grpcPort) {
        this.grpcPort = grpcPort;
    }

    public Integer getInternalGrpcPort() {
        return internalGrpcPort;
    }

    public void setInternalGrpcPort(Integer internalGrpcPort) {
        this.internalGrpcPort = internalGrpcPort;
    }

    public Integer getHttpPort() {
        return httpPort;
    }

    public void setHttpPort(Integer httpPort) {
        this.httpPort = httpPort;
    }

    public boolean isConnected() {
        return connected;
    }

    public void setConnected(boolean connected) {
        this.connected = connected;
    }

    public boolean isMaster() {
        return master;
    }

    public void setMaster(boolean master) {
        this.master = master;
    }

    public String getContext() {
        return context;
    }

    public void setContext(String context) {
        this.context = context;
    }

    public Boolean getNoContexts() {
        return noContexts;
    }

    public void setNoContexts(Boolean noContexts) {
        this.noContexts = noContexts;
    }
}
