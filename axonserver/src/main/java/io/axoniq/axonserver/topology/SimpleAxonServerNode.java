/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.topology;

import java.util.Collection;
import java.util.Collections;

/**
 * @author Marc Gathier
 */
public class SimpleAxonServerNode implements AxonServerNode {

    private final String name;
    private final String hostName;
    private final int port;
    private final int httpPort;

    public SimpleAxonServerNode(String name, String hostName, int port, int httpPort) {
        this.name = name;
        this.hostName = hostName;
        this.port = port;
        this.httpPort = httpPort;
    }

    @Override
    public String getHostName() {
        return hostName;
    }

    @Override
    public Integer getGrpcPort() {
        return port;
    }

    @Override
    public String getInternalHostName() {
        return null;
    }

    @Override
    public Integer getGrpcInternalPort() {
        return 0;
    }

    @Override
    public Integer getHttpPort() {
        return httpPort;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Collection<String> getContextNames() {
        return Collections.singleton(Topology.DEFAULT_CONTEXT);
    }
}
