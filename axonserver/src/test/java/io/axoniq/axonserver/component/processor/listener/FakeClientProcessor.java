/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.component.processor.listener;

import io.axoniq.axonserver.grpc.control.EventProcessorInfo;

/**
 * {@link ClientProcessor} Fake implementation for test purpose .
 *
 * @author Sara Pellegrini
 *
 */
public class FakeClientProcessor implements ClientProcessor {

    private final String clientId;

    private final boolean belongsToComponent;

    private final boolean belongsToContext;

    private final EventProcessorInfo eventProcessorInfo;

    public FakeClientProcessor(String clientId, boolean belongsToComponent, String processorName) {
        this(clientId, belongsToComponent, EventProcessorInfo.newBuilder()
                                                             .setProcessorName(processorName)
                                                             .build());
    }

    public FakeClientProcessor(String clientId, boolean belongsToComponent,
                               EventProcessorInfo eventProcessorInfo) {
        this(clientId, belongsToComponent, true, eventProcessorInfo);
    }

    public FakeClientProcessor(String clientId, boolean belongsToComponent, boolean belongsToContext,
                               EventProcessorInfo eventProcessorInfo) {
        this.clientId = clientId;
        this.belongsToComponent = belongsToComponent;
        this.belongsToContext = belongsToContext;
        this.eventProcessorInfo = eventProcessorInfo;
    }

    @Override
    public String clientName() {
        return clientId;
    }

    @Override
    public EventProcessorInfo eventProcessorInfo() {
        return eventProcessorInfo;
    }

    @Override
    public Boolean belongsToComponent(String component) {
        return belongsToComponent;
    }

    @Override
    public boolean belongsToContext(String context) {
        return belongsToContext;
    }
}
