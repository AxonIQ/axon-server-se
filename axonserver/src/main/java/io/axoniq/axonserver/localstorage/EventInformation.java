/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage;

import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.EventWithToken;

/**
 * @author Marc Gathier
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

    public Event getEvent() {
        return event.asEvent();
    }

    public EventWithToken asEventWithToken() {
        return event.asEventWithToken();
    }

    public SerializedEventWithToken getSerializedEventWithToken() {
        return event;
    }
}
