/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.transport.grpc.eventprocessor;

import io.axoniq.axonserver.admin.eventprocessor.api.EventProcessorId;
import io.axoniq.axonserver.grpc.admin.EventProcessorIdentifier;
import io.axoniq.axonserver.util.StringUtils;

import javax.annotation.Nonnull;

/**
 * Implementation of {@link EventProcessorId} that wraps the Grpc message and accesses to it only if needed.
 *
 * @author Sara Pellegrini
 * @since 4.6
 */
public class EventProcessorIdMessage implements EventProcessorId {

    private final String context;
    private final EventProcessorIdentifier grpcMessage;

    public EventProcessorIdMessage(String context, EventProcessorIdentifier grpcMessage) {
        this.grpcMessage = grpcMessage;
        this.context = context;
    }

    @Nonnull
    @Override
    public String name() {
        return grpcMessage.getProcessorName();
    }


    @Nonnull
    @Override
    public String tokenStoreIdentifier() {
        return grpcMessage.getTokenStoreIdentifier();
    }

    @Nonnull
    @Override
    public String context() {
        return context;
    }
}
