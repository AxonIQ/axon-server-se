/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.admin.eventprocessor.api;

import io.axoniq.axonserver.component.processor.EventProcessorIdentifier;
import io.axoniq.axonserver.topology.Topology;

import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Fake implementation of {@link EventProcessor} for test purpose
 *
 * @author Sara Pellegrini
 */
public class FakeEventProcessor implements EventProcessor {

    private final EventProcessorId id;
    private final boolean streaming;
    private final String mode;
    private final List<EventProcessorInstance> instances;

    public FakeEventProcessor(String processorName,
                              String tokenStoreIdentifier,
                              boolean streaming,
                              String mode,
                              List<EventProcessorInstance> instances) {
        this(new EventProcessorIdentifier(processorName, Topology.DEFAULT_CONTEXT, tokenStoreIdentifier),
             streaming, mode, instances);
    }

    public FakeEventProcessor(EventProcessorId id, boolean streaming, String mode,
                              List<EventProcessorInstance> instances) {
        this.id = id;
        this.streaming = streaming;
        this.mode = mode;
        this.instances = instances;
    }

    @Nonnull
    @Override
    public EventProcessorId id() {
        return id;
    }

    @Override
    public boolean isStreaming() {
        return streaming;
    }

    @Nonnull
    @Override
    public String mode() {
        return mode;
    }

    @Nonnull
    @Override
    public Iterable<EventProcessorInstance> instances() {
        return instances;
    }

    @Nullable
    @Override
    public String loadBalancingStrategyName() {
        return null;
    }
}
