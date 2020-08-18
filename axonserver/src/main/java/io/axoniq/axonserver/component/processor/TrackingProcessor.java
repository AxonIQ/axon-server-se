/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.component.processor;

import io.axoniq.axonserver.component.processor.listener.ClientProcessor;
import io.axoniq.axonserver.component.processor.warning.DuplicatedTrackers;
import io.axoniq.axonserver.component.processor.warning.MissingTrackers;
import io.axoniq.axonserver.component.processor.warning.Warning;
import io.axoniq.axonserver.grpc.control.EventProcessorInfo;
import io.axoniq.axonserver.serializer.Media;
import io.axoniq.axonserver.serializer.Printable;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.collect.Iterables.concat;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;

/**
 * Tracking Event Processor state representation for the UI.
 *
 * @author Sara Pellegrini
 * @since 4.0
 */
public class TrackingProcessor extends GenericProcessor implements EventProcessor {

    private static final int ZERO_THREADS = 0;

    private static final String FREE_THREAD_INSTANCES_COUNT = "freeThreadInstances";
    private static final String ACTIVE_THREADS_COUNT = "activeThreads";
    private static final String CAN_PAUSE_KEY = "canPause";
    private static final String CAN_PLAY_KEY = "canPlay";
    private static final String CAN_SPLIT_KEY = "canSplit";
    private static final String CAN_MERGE_KEY = "canMerge";
    private static final String TRACKERS_LIST_KEY = "trackers";

    private final String tokenStoreIdentifier;

    /**
     * Instantiate a {@link TrackingProcessor}, used to represent the state of a Tracking Event Processor in the UI.
     *
     * @param name       a {@link String} defining the processing group name of this Event Processor
     * @param mode       a {@link String} defining the mode of this Event Processor
     * @param processors a {@link Collection} of {@link ClientProcessor}s portraying the state of this Event Processor
     *                   per client it is running on
     */
    TrackingProcessor(String name, String mode, Collection<ClientProcessor> processors) {
        super(name, mode, processors);
        this.tokenStoreIdentifier = processors.iterator().next().eventProcessorInfo().getTokenStoreIdentifier();
    }

    @Override
    public String fullName() {
        return name() + "@" + tokenStoreIdentifier;
    }

    @Override
    public Iterable<Warning> warnings() {
        return asList(
                new DuplicatedTrackers(concat(processors())),
                new MissingTrackers(concat(processors()))
        );
    }

    @Override
    public void printOn(Media media) {
        super.printOn(media);
        media.with("tokenStoreIdentifier", tokenStoreIdentifier);

        Set<String> freeThreadInstances =
                processors().stream()
                            .filter(processor -> processor.eventProcessorInfo().getAvailableThreads() > ZERO_THREADS)
                            .map(ClientProcessor::clientName)
                            .collect(Collectors.toSet());

        Integer activeThreads = processorInstances().stream()
                                                    .map(EventProcessorInfo::getActiveThreads)
                                                    .reduce(Integer::sum)
                                                    .orElse(0);

        boolean isRunning = processors().stream().anyMatch(ClientProcessor::running);

        int largestSegmentFactor = processorInstances().stream()
                                                       .map(EventProcessorInfo::getSegmentStatusList)
                                                       .flatMap(List::stream)
                                                       .mapToInt(EventProcessorInfo.SegmentStatus::getOnePartOf)
                                                       .min()
                                                       .orElse(1);
        boolean canMerge = isRunning && largestSegmentFactor != 1;

        media.withStrings(FREE_THREAD_INSTANCES_COUNT, freeThreadInstances)
             .with(ACTIVE_THREADS_COUNT, activeThreads)
             .with(CAN_PAUSE_KEY, isRunning)
             .with(CAN_PLAY_KEY, processors().stream().anyMatch(p -> !p.running()))
             .with(CAN_SPLIT_KEY, isRunning)
             .with(CAN_MERGE_KEY, canMerge)
             .with(TRACKERS_LIST_KEY, trackers());
    }

    private List<Printable> trackers() {
        return processors()
                .stream()
                .flatMap(client -> client.eventProcessorInfo().getSegmentStatusList().stream()
                                         .map(tracker -> new TrackingProcessorSegment(client.clientName(), tracker)))
                .sorted(Comparator.comparingInt(TrackingProcessorSegment::segmentId).thenComparing(TrackingProcessorSegment::clientId))
                .collect(toList());
    }

    private List<EventProcessorInfo> processorInstances() {
        return processors().stream().map(ClientProcessor::eventProcessorInfo).collect(toList());
    }
}
