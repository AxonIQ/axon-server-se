/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.transaction;

import io.axoniq.axonserver.localstorage.transformation.ProcessedEvent;

import java.util.List;

/**
 * @author Marc Gathier
 */
public class PreparedTransaction {
    private final long token;
    private final List<ProcessedEvent> eventList;

    public PreparedTransaction(long token, List<ProcessedEvent> eventList) {
        this.token = token;
        this.eventList = eventList;
    }

    public long getToken() {
        return token;
    }

    public List<ProcessedEvent> getEventList() {
        return eventList;
    }

}
