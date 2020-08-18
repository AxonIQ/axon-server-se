/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.component.processor;

import io.axoniq.axonserver.grpc.control.EventProcessorInfo;

/**
 * Holder for event processor information for a client.
 * author Marc Gathier
 */
public class ClientEventProcessorInfo {

    private final String clientName;
    private final String context;
    private final EventProcessorInfo eventProcessorInfo;

    public ClientEventProcessorInfo(String clientName, String context, EventProcessorInfo eventProcessorInfo) {

        this.clientName = clientName;
        this.context = context;
        this.eventProcessorInfo = eventProcessorInfo;
    }

    public String getClientId() {
        return clientName;
    }

    public String getContext() {
        return context;
    }

    public EventProcessorInfo getEventProcessorInfo() {
        return eventProcessorInfo;
    }
}
