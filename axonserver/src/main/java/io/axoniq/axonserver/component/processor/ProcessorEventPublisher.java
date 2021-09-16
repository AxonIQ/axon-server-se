/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.component.processor;

import io.axoniq.axonserver.applicationevents.EventProcessorEvents.EventProcessorStatusUpdate;
import io.axoniq.axonserver.applicationevents.EventProcessorEvents.MergeSegmentRequest;
import io.axoniq.axonserver.applicationevents.EventProcessorEvents.PauseEventProcessorRequest;
import io.axoniq.axonserver.applicationevents.EventProcessorEvents.ReleaseSegmentRequest;
import io.axoniq.axonserver.applicationevents.EventProcessorEvents.SplitSegmentRequest;
import io.axoniq.axonserver.applicationevents.EventProcessorEvents.StartEventProcessorRequest;
import io.axoniq.axonserver.component.processor.listener.ClientProcessor;
import io.axoniq.axonserver.component.processor.listener.ClientProcessors;
import io.axoniq.axonserver.grpc.PlatformService;
import io.axoniq.axonserver.grpc.control.EventProcessorInfo.SegmentStatus;
import io.axoniq.axonserver.grpc.control.PlatformInboundInstruction;
import org.jetbrains.annotations.NotNull;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.annotation.PostConstruct;

import static io.axoniq.axonserver.grpc.control.PlatformInboundInstruction.RequestCase.EVENT_PROCESSOR_INFO;

/**
 * A service to be able to publish specific application events for Event Processor instances, like
 * {@link #startProcessorRequest(String, String)}, {@link #pauseProcessorRequest(String, String)} and
 * {@link #releaseSegment(String, String, int)}.
 *
 * @author Sara Pellegrini
 * @since 4.0
 */
@Component
public class ProcessorEventPublisher {

    private static final boolean REVERSE_ORDER = true;
    private static final boolean REGULAR_ORDER = false;
    private static final boolean NOT_PROXIED = false;
    private static final boolean PARALLELIZE_STREAM = false;

    private final PlatformService platformService;
    private final ApplicationEventPublisher applicationEventPublisher;
    private final ClientProcessors clientProcessors;

    /**
     * Instantiate a service which published Event Processor specific application events throughout this Axon Server
     * instance.
     *
     * @param platformService           the {@link PlatformService} used to register an inbound instruction on to update
     *                                  Event Processor information
     * @param applicationEventPublisher the {@link ApplicationEventPublisher} used to publish the Event Processor
     *                                  specific application events
     * @param clientProcessors          an {@link Iterable} of {@link ClientProcessor}instances used to deduce the
     *                                  segment to be split/merged on the {@link #splitSegment(List, String)}
     *                                  and {@link #mergeSegment(List, String)} operations.
     */
    public ProcessorEventPublisher(PlatformService platformService,
                                   ApplicationEventPublisher applicationEventPublisher,
                                   ClientProcessors clientProcessors) {
        this.platformService = platformService;
        this.applicationEventPublisher = applicationEventPublisher;
        this.clientProcessors = clientProcessors;
    }

    @PostConstruct
    public void init() {
        platformService.onInboundInstruction(EVENT_PROCESSOR_INFO, this::publishEventProcessorStatus);
    }

    private void publishEventProcessorStatus(PlatformService.ClientComponent clientComponent,
                                             PlatformInboundInstruction inboundInstruction) {
        ClientEventProcessorInfo processorStatus =
                new ClientEventProcessorInfo(clientComponent.getClientId(),
                                             clientComponent.getClientStreamId(),
                                             inboundInstruction.getEventProcessorInfo());
        applicationEventPublisher.publishEvent(new EventProcessorStatusUpdate(processorStatus));
    }

    public void pauseProcessorRequest(String clientId, String processorName) {
        applicationEventPublisher.publishEvent(new PauseEventProcessorRequest(clientId, processorName, NOT_PROXIED));
    }

    public void startProcessorRequest(String clientId, String processorName) {
        applicationEventPublisher.publishEvent(new StartEventProcessorRequest(clientId, processorName, NOT_PROXIED));
    }

    public void releaseSegment(String clientId, String processorName, int segmentId) {
        applicationEventPublisher.publishEvent(
                new ReleaseSegmentRequest(clientId, processorName, segmentId, NOT_PROXIED)
        );
    }

    /**
     * Split the biggest segment of the given {@code processorName} in two, by publishing a {@link SplitSegmentRequest}
     * as an application event to be picked up by the component publishing this message towards the right Axon client.
     *
     * @param clientIds     a {@link List} of {@link String}s containing the specified tracking event processor
     * @param processorName a {@link String} specifying the Tracking Event Processor for which the biggest segment
     *                      should be split in two
     */
    public void splitSegment(List<String> clientIds, String processorName) {
        Map<ClientSegmentPair, SegmentStatus> clientToTracker =
                buildClientToTrackerMap(clientIds, processorName, REGULAR_ORDER);

        Integer biggestSegment = clientToTracker.values().stream()
                                                .min(Comparator.comparingInt(SegmentStatus::getOnePartOf))
                                                .map(SegmentStatus::getSegmentId)
                                                .orElseThrow(() -> new IllegalArgumentException(
                                                        "No segments found for processor name [" + processorName + "]"
                                                ));

        applicationEventPublisher.publishEvent(new SplitSegmentRequest(
                NOT_PROXIED,
                getClientIdForSegment(clientToTracker, biggestSegment)
                        .orElseThrow(() -> new IllegalArgumentException("No client found which has a claim on segment [" + biggestSegment + "]")),
                processorName,
                biggestSegment
        ));
    }

    /**
     * Merge the smallest segment of the given {@code processorName} with the segment that it is paired with, by
     * publishing a {@link MergeSegmentRequest} as an application event to be picked up by the component publishing this
     * message towards the right Axon client. Prior to publishing the {@code MergeSegmentRequest} firstly all other
     * active clients should be notified to release the paired segment.
     *
     * @param clientIds     a {@link List} of {@link String}s containing the specified tracking event processor
     * @param processorName a {@link String} specifying the Tracking Event Processor for which the smallest segment
     *                      should be merged with the segment that it's paired with
     */
    public void mergeSegment(List<String> clientIds, String processorName) {
        Map<ClientSegmentPair, SegmentStatus> clientToTracker =
                buildClientToTrackerMap(clientIds, processorName, REVERSE_ORDER);

        SegmentStatus smallestSegment =
                clientToTracker.values().stream()
                               .max(Comparator.comparingInt(SegmentStatus::getOnePartOf))
                               .orElseThrow(() -> new IllegalArgumentException(
                                       "No segments found for processor name [" + processorName + "]"
                               ));

        int segmentToMerge = deduceSegmentToMerge(smallestSegment);
        Optional<String> clientIdOwningSegmentToMerge = getClientIdForSegment(clientToTracker, segmentToMerge);

        clientIdOwningSegmentToMerge.ifPresent(
                clientId -> clientIds.stream()
                                     .filter(client -> !client.equals(clientId))
                                     .forEach(client -> releaseSegment(client,
                                                                       processorName,
                                                                       smallestSegment.getSegmentId()))
        );

        if (clientIdOwningSegmentToMerge.isPresent()) {
            applicationEventPublisher.publishEvent(new MergeSegmentRequest(
                    NOT_PROXIED, clientIdOwningSegmentToMerge.get(), processorName, segmentToMerge
            ));
        } else {
            // the segment to merge with is unclaimed. We need to merge the known part
            String clientOwningSmallestSegment = getClientIdForSegment(clientToTracker, smallestSegment.getSegmentId())
                    .orElseThrow(() -> new IllegalArgumentException(
                            "Attempt to merge segments [" + segmentToMerge + "] and [" + smallestSegment.getSegmentId()
                                    + "] failed")
                    );

            applicationEventPublisher.publishEvent(new MergeSegmentRequest(
                    NOT_PROXIED, clientOwningSmallestSegment, processorName, smallestSegment.getSegmentId()
            ));
        }
    }

    /**
     * Build a convenience {@link TreeMap}, where the key is a {@link ClientSegmentPair} and the value is an
     * {@link SegmentStatus}, to be used to support the {@link #splitSegment(List, String)} and
     * {@link #mergeSegment(List, String)} operations.
     *
     * @param clientIds     a {@link List} of {@link String}s specifying the clients to take into account when building
     *                      the {@link Map}
     * @param processorName a {@link String} specifying the Tracking Event Processor for which the supported split/merge
     *                      operation should be executed
     * @param reverseOrder  a {@code boolean} specifying whether the returned {@link Map} should following the regular
     *                      ordering or if it should be reversed
     * @return a {@link Map} of {@link ClientSegmentPair} to {@link SegmentStatus} to support the
     * {@link #splitSegment(List, String)} and {@link #mergeSegment(List, String)} operations
     */
    @NotNull
    private Map<ClientSegmentPair, SegmentStatus> buildClientToTrackerMap(List<String> clientIds,
                                                                          String processorName,
                                                                          boolean reverseOrder) {
        Map<ClientSegmentPair, SegmentStatus> clientToTracker =
                reverseOrder ? new TreeMap<>(Collections.reverseOrder()) : new TreeMap<>();

        List<ClientProcessor> clientsWithProcessor =
                StreamSupport.stream(clientProcessors.spliterator(), PARALLELIZE_STREAM)
                             .filter(clientProcessor -> clientIds.contains(clientProcessor.clientId()))
                             .filter(clientProcessor -> clientProcessor.eventProcessorInfo().getProcessorName()
                                                                       .equals(processorName))
                             .collect(Collectors.toList());

        for (ClientProcessor clientProcessor : clientsWithProcessor) {
            List<SegmentStatus> eventTrackers = clientProcessor.eventProcessorInfo().getSegmentStatusList();
            eventTrackers.forEach(eventTracker -> clientToTracker.put(
                    new ClientSegmentPair(clientProcessor.clientId(), eventTracker.getSegmentId()), eventTracker)
            );
        }

        return clientToTracker;
    }

    private Optional<String> getClientIdForSegment(Map<ClientSegmentPair, SegmentStatus> clientToTracker,
                                                   Integer segmentId) {
        return clientToTracker.keySet().stream()
                              .filter(clientAndSegment -> clientAndSegment.getSegmentId() == segmentId)
                              .findFirst()
                              .map(ClientSegmentPair::getClientId);
    }

    /**
     * Deduce the segment id with which the given {@code segment} should be merged. Do so, by deducing the "parent mask"
     * by bit shifting the segment's size (the {@link SegmentStatus#getOnePartOf()} field) by one, and doing an XOR
     * on the given segment's {@link SegmentStatus#getSegmentId()} with the deduced "parent mask".
     *
     * @param segment a {@link SegmentStatus} for which to deduce the segment to merge with
     * @return an {@code int} specifying the segment identifier to merge with the given {@code segment}
     */
    private int deduceSegmentToMerge(SegmentStatus segment) {
        int segmentSize = segment.getOnePartOf();
        int parentMask = segmentSize >>> 1;
        return segment.getSegmentId() ^ parentMask;
    }

    /**
     * Pair of {@code clientId} and {@code segmentId}, used to simplify the split and merge logic by combining the two
     * main fields required to issue a split/merge operation.
     */
    private static class ClientSegmentPair implements Comparable<ClientSegmentPair> {

        private final String clientId;
        private final int segmentId;

        /**
         * Instantiate a {@link ClientSegmentPair}, using the given {@code clientId} and {@code segmentId} as it's
         * contents.
         *
         * @param clientId  a {@link String} specifying the identifier of the client
         * @param segmentId a {@code int} specifying the identifier of the segment
         */
        private ClientSegmentPair(String clientId, int segmentId) {
            this.clientId = clientId;
            this.segmentId = segmentId;
        }

        public String getClientId() {
            return clientId;
        }

        public int getSegmentId() {
            return segmentId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(clientId, segmentId);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            final ClientSegmentPair other = (ClientSegmentPair) obj;
            return Objects.equals(this.clientId, other.clientId)
                    && Objects.equals(this.segmentId, other.segmentId);
        }

        @Override
        public int compareTo(@NotNull ClientSegmentPair other) {
            return Integer.compare(this.segmentId, other.segmentId);
        }
    }
}
