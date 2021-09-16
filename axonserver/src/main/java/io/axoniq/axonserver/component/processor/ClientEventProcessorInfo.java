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
 *
 * @author Marc Gathier
 */
public class ClientEventProcessorInfo {

    private final String clientId;
    private final String clientStreamId;
    private final EventProcessorInfo eventProcessorInfo;

    public ClientEventProcessorInfo(String clientId, String clientStreamId,
                                    EventProcessorInfo eventProcessorInfo) {

        this.clientId = clientId;
        this.clientStreamId = clientStreamId;
        this.eventProcessorInfo = eventProcessorInfo;
    }

    /**
     * Returns the identifier of the client instance.
     *
     * @return the identifier of the client instance.
     */
    public String getClientId() {
        return clientId;
    }

    /**
     * Returns the identifier of the instruction stream used by the client to connect to the server.
     *
     * @return the identifier of the instruction stream used by the client to connect to the server.
     */
    public String getClientStreamId() {
        return clientStreamId;
    }

    /**
     * Returns the information about the event processor.
     *
     * @return the information about the event processor.
     */
    public EventProcessorInfo getEventProcessorInfo() {
        return eventProcessorInfo;
    }
}
