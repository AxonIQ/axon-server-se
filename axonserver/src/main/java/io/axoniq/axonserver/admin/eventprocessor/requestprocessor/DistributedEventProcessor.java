/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.admin.eventprocessor.requestprocessor;

import com.google.common.collect.Iterables;
import io.axoniq.axonserver.admin.eventprocessor.api.EventProcessor;
import io.axoniq.axonserver.admin.eventprocessor.api.EventProcessorId;
import io.axoniq.axonserver.admin.eventprocessor.api.EventProcessorInstance;
import io.axoniq.axonserver.admin.eventprocessor.api.EventProcessorSegment;
import io.axoniq.axonserver.component.processor.listener.ClientProcessor;
import io.axoniq.axonserver.grpc.control.EventProcessorInfo;
import io.axoniq.axonserver.grpc.control.EventProcessorInfo.SegmentStatus;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


/**
 * {@link io.axoniq.axonserver.admin.eventprocessor.api.EventProcessor} implementation based on a collection of
 * {@link ClientProcessor}s
 *
 * @author Sara Pellegrini
 * @since 4.6.0
 */
public class DistributedEventProcessor implements EventProcessor {

    private static final String TRACKING_EVENT_PROCESSOR_MODE = "Tracking";

    private final EventProcessorId id;
    private final List<ClientProcessor> clientProcessors;

    /**
     * Constructs an instance based on the name of the event processor * @param clientProcessors
     */
    public DistributedEventProcessor(EventProcessorId id, List<ClientProcessor> clientProcessors) {
        this.id = id;
        this.clientProcessors = new LinkedList<>(clientProcessors);
    }

    @Nonnull
    @Override
    public EventProcessorId id() {
        return id;
    }

    @Nonnull
    @Override
    public String mode() {
        Set<String> modes = clientProcessors.stream().map(ClientProcessor::eventProcessorInfo)
                                            .map(EventProcessorInfo::getMode).collect(Collectors.toSet());
        return modes.size() == 1 ? modes.iterator().next() : "Unknown";
    }

    @Nonnull
    @Override
    public Iterable<EventProcessorInstance> instances() {
        return clientProcessors.stream().map(ClientInstance::new).collect(Collectors.toList());
    }

    @Override
    public boolean isStreaming() {
        return clientProcessors.stream()
                               .map(ClientProcessor::eventProcessorInfo)
                               .map(this::isStreamingProcessor)
                               .reduce(Boolean::logicalAnd)
                               .orElse(Boolean.FALSE);
    }

    private boolean isStreamingProcessor(EventProcessorInfo eventProcessorInfo) {
        return eventProcessorInfo.getIsStreamingProcessor()
                || TRACKING_EVENT_PROCESSOR_MODE.equals(eventProcessorInfo.getMode());
    }

    @Nullable
    @Override
    public String loadBalancingStrategyName() {
        return null;
    }

    private static class ClientInstance implements EventProcessorInstance {

        private final ClientProcessor clientProcessor;

        ClientInstance(ClientProcessor clientProcessor) {
            this.clientProcessor = clientProcessor;
        }

        @Nonnull
        @Override
        public String clientId() {
            return clientProcessor.clientId();
        }

        @Override
        public boolean isRunning() {
            return clientProcessor.running();
        }

        @Override
        public int maxCapacity() {
            return clientProcessor.eventProcessorInfo().getAvailableThreads() + Iterables.size(clientProcessor);
        }

        @Nonnull
        @Override
        public Iterable<EventProcessorSegment> claimedSegments() {
            List<EventProcessorSegment> segments = new LinkedList<>();
            clientProcessor.forEach(
                    segmentStatus -> segments.add(new Segment(clientProcessor.clientId(), segmentStatus)));
            return segments;
        }
    }

    private static class Segment implements EventProcessorSegment {

        private final String clientId;
        private final SegmentStatus segmentStatus;

        Segment(String clientId, SegmentStatus segmentStatus) {
            this.clientId = clientId;
            this.segmentStatus = segmentStatus;
        }


        @Override
        public int id() {
            return segmentStatus.getSegmentId();
        }

        @Override
        public int onePartOf() {
            return segmentStatus.getOnePartOf();
        }

        @Nonnull
        @Override
        public String claimedBy() {
            return clientId;
        }

        @Override
        public boolean isCaughtUp() {
            return segmentStatus.getCaughtUp();
        }

        @Override
        public boolean isReplaying() {
            return segmentStatus.getReplaying();
        }

        @Override
        public long tokenPosition() {
            return segmentStatus.getTokenPosition();
        }

        @Override
        public boolean isInError() {
            return !segmentStatus.getErrorState().isEmpty();
        }

        @Nonnull
        @Override
        public Optional<String> error() {
            return isInError() ? Optional.of(segmentStatus.getErrorState()) : Optional.empty();
        }
    }
}

