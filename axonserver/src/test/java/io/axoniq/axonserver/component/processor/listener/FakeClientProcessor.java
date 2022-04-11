/*
 * Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.component.processor.listener;

import io.axoniq.axonserver.grpc.control.EventProcessorInfo;
import io.axoniq.axonserver.topology.Topology;

import java.util.Arrays;

/**
 * {@link ClientProcessor} Fake implementation for test purpose .
 *
 * @author Sara Pellegrini
 */
public class FakeClientProcessor implements ClientProcessor {

    private final String clientId;

    private final boolean belongsToComponent;

    private final String context;

    private final EventProcessorInfo eventProcessorInfo;

    public FakeClientProcessor(String clientId, boolean belongsToComponent, String processorName, boolean running) {
        this(clientId, belongsToComponent, EventProcessorInfo.newBuilder()
                                                             .setProcessorName(processorName)
                                                             .setRunning(running)
                                                             .build());
    }

    public FakeClientProcessor(String clientId, boolean belongsToComponent,
                               EventProcessorInfo eventProcessorInfo) {
        this(clientId, belongsToComponent, Topology.DEFAULT_CONTEXT, eventProcessorInfo);
    }

    public FakeClientProcessor(String clientId, boolean belongsToComponent, String context,
                               EventProcessorInfo eventProcessorInfo) {
        this.clientId = clientId;
        this.belongsToComponent = belongsToComponent;
        this.context = context;
        this.eventProcessorInfo = eventProcessorInfo;
    }

    public FakeClientProcessor(String clientId, String processorName, String tokenStore,
                               EventProcessorInfo.SegmentStatus... segments) {
        this(clientId, processorName, tokenStore, Topology.DEFAULT_CONTEXT, segments);
    }

    public FakeClientProcessor(String clientId, String processorName, String tokenStore, String context,
                               EventProcessorInfo.SegmentStatus... segments) {
        this(clientId, true, context, EventProcessorInfo.newBuilder()
                                                        .setProcessorName(processorName)
                                                        .setTokenStoreIdentifier(tokenStore)
                                                        .addAllSegmentStatus(Arrays.asList(segments))
                                                        .setAvailableThreads(10)
                                                        .setRunning(true)
                                                        .build());
    }

    @Override
    public String clientId() {
        return clientId;
    }

    @Override
    public String context() {
        return context;
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
        return this.context.equals(context);
    }

    public FakeClientProcessor withAvailableThreads(int availableThreads) {
        return new FakeClientProcessor(clientId,
                                       belongsToComponent,
                                       context,
                                       eventProcessorInfo.toBuilder()
                                                         .setAvailableThreads(availableThreads)
                                                         .build());
    }

    public FakeClientProcessor withActiveThreads(int activeThreads) {
        return new FakeClientProcessor(clientId,
                                       belongsToComponent,
                                       context,
                                       eventProcessorInfo.toBuilder()
                                                         .setActiveThreads(activeThreads)
                                                         .build());
    }
}
