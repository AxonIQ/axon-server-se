/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage;

import io.axoniq.axonserver.grpc.event.EventWithToken;

/**
 * Decorator that allows changing the events before returning them at the client. This decorator is applied on
 * the node where the event is read.
 *
 * @author Marc Gathier
 * @since 4.4
 */
public interface EventDecorator {

    /**
     * Manipulate the event before returning it to the client.
     *
     * @param event the original event
     * @return the updated event
     */
    default SerializedEvent decorateEvent(SerializedEvent event) {
        return event;
    }

    /**
     * Manipulate the event with token before returning it to the client.
     *
     * @param eventWithToken the original event with token
     * @return the updated event
     */
    default SerializedEventWithToken decorateEventWithToken(SerializedEventWithToken eventWithToken) {
        return eventWithToken;
    }

    /**
     * Manipulate the event with token before returning it to the client.
     *
     * @param event the original event with token
     * @return the updated event
     */
    default EventWithToken decorateEventWithToken(EventWithToken event) {
        return event;
    }
}
