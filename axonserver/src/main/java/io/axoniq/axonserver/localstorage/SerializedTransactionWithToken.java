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

import java.util.List;
import java.util.Objects;

/**
 * @author Marc Gathier
 */
public class SerializedTransactionWithToken {

    private final long token;
    private final byte version;
    private final List<SerializedEvent> events;

    public SerializedTransactionWithToken(long token, byte version,
                                          List<SerializedEvent> events) {
        this.token = token;
        this.version = version;
        this.events = events;
    }

    public long getToken() {
        return token;
    }

    public List<SerializedEvent> getEvents() {
        return events;
    }

    public int getVersion() {
        return version;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SerializedTransactionWithToken that = (SerializedTransactionWithToken) o;
        return token == that.token &&
                Objects.equals(events, that.events);
    }

    @Override
    public int hashCode() {
        return Objects.hash(token, events);
    }

    public int getEventsCount() {
        return events.size();
    }

    public Event getEvents(int i) {
        return events.get(i).asEvent();
    }
}
