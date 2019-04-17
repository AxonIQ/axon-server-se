/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.topology;

import io.axoniq.axonserver.config.MessagingPlatformConfiguration;

/**
 * Implementation of {@link Topology} for Standard Edition. Only contains the current node.
 * @author Marc Gathier
 */
public class DefaultTopology implements Topology {
    private final AxonServerNode me;

    public DefaultTopology(MessagingPlatformConfiguration configuration) {
        this(new SimpleAxonServerNode(configuration.getName(), configuration.getFullyQualifiedHostname(), configuration.getPort(),configuration.getHttpPort()));
    }

    public DefaultTopology(AxonServerNode me) {
        this.me = me;
    }

    @Override
    public String getName() {
        return me.getName();
    }

    @Override
    public AxonServerNode getMe() {
        return me;
    }
}
