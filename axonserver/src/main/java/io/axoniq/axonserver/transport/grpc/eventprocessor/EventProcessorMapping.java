/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.transport.grpc.eventprocessor;

import io.axoniq.axonserver.admin.eventprocessor.api.EventProcessor;
import io.axoniq.axonserver.grpc.admin.EventProcessorIdentifier;
import io.axoniq.axonserver.grpc.admin.EventProcessorInstance;
import io.axoniq.axonserver.grpc.admin.EventProcessorSegment;

import java.util.function.Function;

/**
 * Transforms an {@link EventProcessor} into the corresponding GRPC message.
 *
 * @author Sara Pellegrini
 * @since 4.6.0
 */
public class EventProcessorMapping
        implements Function<EventProcessor, io.axoniq.axonserver.grpc.admin.EventProcessor> {

    @Override
    public io.axoniq.axonserver.grpc.admin.EventProcessor apply(EventProcessor processor) {
        io.axoniq.axonserver.grpc.admin.EventProcessor.Builder builder = io.axoniq.axonserver.grpc.admin.EventProcessor.newBuilder();
        builder.setIdentifier(EventProcessorIdentifier.newBuilder()
                                                      .setProcessorName(processor.id().name())
                                                      .setTokenStoreIdentifier(processor.id().tokenStoreIdentifier()));
        builder.setMode(processor.mode())
               .setIsStreaming(processor.isStreaming());

        if( processor.loadBalancingStrategyName() != null ) {
            builder.setLoadBalancingStrategyName(processor.loadBalancingStrategyName());
        }

        processor.instances().forEach(instance -> builder.addClientInstance(grpcProcessorInstance(instance)));
        return builder.build();
    }

    private EventProcessorInstance grpcProcessorInstance(
            io.axoniq.axonserver.admin.eventprocessor.api.EventProcessorInstance instance) {
        EventProcessorInstance.Builder builder = EventProcessorInstance.newBuilder();
        builder.setClientId(instance.clientId())
               .setIsRunning(instance.isRunning())
               .setMaxCapacity(instance.maxCapacity());
        instance.claimedSegments().forEach(segment -> builder.addClaimedSegment(grpcSegment(segment)));
        return builder.build();
    }

    private EventProcessorSegment grpcSegment(
            io.axoniq.axonserver.admin.eventprocessor.api.EventProcessorSegment segment) {
        EventProcessorSegment.Builder builder = EventProcessorSegment.newBuilder();
        return builder.setId(segment.id())
                      .setClaimedBy(segment.claimedBy())
                      .setIsCaughtUp(segment.isCaughtUp())
                      .setIsReplaying(segment.isReplaying())
                      .setIsInError(segment.isInError())
                      .setOnePartOf(segment.onePartOf())
                      .setError(segment.error().orElse(""))
                      .build();
    }
}
