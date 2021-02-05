/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.EventWithToken;
import io.axoniq.axonserver.localstorage.SerializedEventWithToken;

/**
 * Holds event with token and its position in the event store segment.
 * @author Marc Gathier
 * @since 4.0
 */
public class EventInformation {

    private final int position;
    private final SerializedEventWithToken event;

    public EventInformation(int position, SerializedEventWithToken event) {
        this.position = position;
        this.event = event;
    }

    public long getToken() {
        return event.getToken();
    }

    public int getPosition() {
        return position;
    }

    /**
     * Determines if the contained event is a domain event (has aggregate information in it).
     * @return true if the event is a domain event
     */
    public boolean isDomainEvent() {
        return event.getSerializedEvent().isDomainEvent();
    }

    public Event getEvent() {
        return event.asEvent();
    }

    public EventWithToken asEventWithToken(boolean snapshot) {
        EventWithToken eventWithToken = event.asEventWithToken();
        if (snapshot) {
            eventWithToken = EventWithToken.newBuilder()
                                           .setToken(eventWithToken.getToken())
                                           .setEvent(Event.newBuilder(eventWithToken.getEvent())
                                                          .setSnapshot(true))
                                           .build();
        }
        return eventWithToken;
    }

    public SerializedEventWithToken getSerializedEventWithToken() {
        return event;
    }
}
