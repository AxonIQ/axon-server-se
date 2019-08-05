/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.topology;

import io.axoniq.axonserver.localstorage.LocalEventStore;
import io.axoniq.axonserver.message.event.EventStore;

import javax.annotation.PostConstruct;

/**
 * Default implementation for an EventStoreLocator.
 * @author Marc Gathier
 * @since 4.0
 */
public class DefaultEventStoreLocator implements EventStoreLocator {
    private final LocalEventStore localEventStore;

    public DefaultEventStoreLocator(LocalEventStore localEventStore) {
        this.localEventStore = localEventStore;
    }

    @PostConstruct
    public void init() {
        localEventStore.initContext(Topology.DEFAULT_CONTEXT, false);
    }

    @Override
    public boolean isLeader(String nodeName, String contextName, boolean waitForLeader) {
        return true;
    }

    @Override
    public EventStore getEventStore(String context) {
        if( Topology.DEFAULT_CONTEXT.equals(context))
            return localEventStore;
        return null;
    }

}
