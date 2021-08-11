/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.topology;

import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.message.event.EventStore;
import reactor.core.publisher.Mono;

/**
 * Defines an interface to retrieve an event store for a context. Standard Edition only supports context "default", and
 * as it does not support clustering, the current node will always be master for the default context.

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

    default Mono<EventStore> eventStore(String context) {
        EventStore eventStore = getEventStore(context);
        return eventStore == null ? Mono.error(new MessagingPlatformException(ErrorCode.NO_EVENTSTORE, "Event store not found")) : Mono.just(eventStore);
    }

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

    default Mono<EventStore> eventStore(String context, boolean forceLeader) {
        EventStore eventStore = getEventStore(context, forceLeader);
        return eventStore == null ? Mono.error(new MessagingPlatformException(ErrorCode.NO_EVENTSTORE, "Event store not found")) : Mono.just(eventStore);
    }

}
