/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.refactoring.store;

import io.axoniq.axonserver.refactoring.store.api.EventStore;

/**
 * Defines an interface to retrieve an event store for a context. Standard Edition only supports context "default", and
 * as it does not support clustering, the current node will always be master for the default context.
 *
 * @author Marc Gathier
 * @since 4.0
 */
public interface EventStoreLocator {

    /**
     * Retrieve an EventStore instance which can be used to store and retrieve events. Returns null when there is no
     * leader for the specified context.
     *
     * @param context the context to get the eventstore for
     * @return an EventStore
     */
    EventStore getEventStore(String context);

    /**
     * Retrieve an EventStore instance which can be used to store and retrieve events. Returns null when there is no
     * leader for the specified context.
     *
     * @param context     the context to get the local EventStore for
     * @param forceLeader use local event store (if possible - if current node has event store for this context,
     *                    otherwise opens a remote connection)
     * @return an EventStore
     */
    default EventStore getEventStore(String context, boolean forceLeader) {
        return getEventStore(context);
    }
}
