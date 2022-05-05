/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.transport.rest.json;

import io.axoniq.axonserver.admin.eventprocessor.api.EventProcessorSegment;
import io.axoniq.axonserver.serializer.Media;
import io.axoniq.axonserver.serializer.Printable;
import io.axoniq.axonserver.transport.rest.json.warning.DuplicatedTrackers;
import io.axoniq.axonserver.transport.rest.json.warning.MissingTrackers;
import io.axoniq.axonserver.transport.rest.json.warning.Warning;

import javax.annotation.Nonnull;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static java.lang.Integer.min;
import static java.util.Arrays.asList;

/**
 * Streaming Event Processor state representation for the UI.
 *
 * @author Sara Pellegrini
 * @since 4.0
 */
public class StreamingProcessor extends GenericProcessor implements Printable {

    private static final String TOKEN_STORE_IDENTIFIER = "tokenStoreIdentifier";
    private static final String CONTEXT = "context";
    private static final String FREE_THREAD_INSTANCES_COUNT = "freeThreadInstances";
    private static final String ACTIVE_THREADS_COUNT = "activeThreads";
    private static final String CAN_PAUSE_KEY = "canPause";
    private static final String CAN_PLAY_KEY = "canPlay";
    private static final String CAN_SPLIT_KEY = "canSplit";
    private static final String CAN_MERGE_KEY = "canMerge";
    private static final String TRACKERS_LIST_KEY = "trackers";

    private final io.axoniq.axonserver.admin.eventprocessor.api.EventProcessor eventProcessor;

    /**
     * Creates an instance base on the {@link io.axoniq.axonserver.admin.eventprocessor.api.EventProcessor}.
     *
     * @param eventProcessor the source of information about the event processor.
     */
    public StreamingProcessor(io.axoniq.axonserver.admin.eventprocessor.api.EventProcessor eventProcessor) {
        super(eventProcessor);
        this.eventProcessor = eventProcessor;
    }

    @Override
    public String fullName() {
        return name() + "@" + tokenStoreIdentifier();
    }

    private String tokenStoreIdentifier() {
        return eventProcessor.id().tokenStoreIdentifier();
    }

    private String context() {
        return eventProcessor.id().context();
    }

    @Override
    public Iterable<Warning> warnings() {
        return asList(new DuplicatedTrackers(segments()), new MissingTrackers(segments()));
    }

    @Override
    public void printOn(Media media) {
        super.printOn(media);


        Set<String> freeThreadInstances = new HashSet<>();
        AtomicBoolean anyClientRunning = new AtomicBoolean();
        AtomicBoolean anyClientPaused = new AtomicBoolean();
        AtomicLong activeThreads = new AtomicLong();
        AtomicInteger largestSegmentFactor = new AtomicInteger(Integer.MAX_VALUE);
        eventProcessor.instances().forEach(instance -> {
            anyClientRunning.set(anyClientRunning.get() || instance.isRunning());
            anyClientPaused.set(anyClientPaused.get() || !instance.isRunning());
            AtomicInteger claimedSegments = new AtomicInteger();
            instance.claimedSegments().forEach(segment -> {
                claimedSegments.incrementAndGet();
                largestSegmentFactor.set(min(largestSegmentFactor.get(), segment.onePartOf()));
            });
            activeThreads.addAndGet(claimedSegments.get());
            if (claimedSegments.get() < instance.maxCapacity()) {
                freeThreadInstances.add(instance.clientId());
            }
        });

        boolean canMerge = anyClientRunning.get() && largestSegmentFactor.get() != 1;

        media.with(TOKEN_STORE_IDENTIFIER, tokenStoreIdentifier())
             .with(CONTEXT, context())
             .withStrings(FREE_THREAD_INSTANCES_COUNT, freeThreadInstances)
             .with(ACTIVE_THREADS_COUNT, activeThreads)
             .with(CAN_PAUSE_KEY, anyClientRunning.get())
             .with(CAN_PLAY_KEY, anyClientPaused.get())
             .with(CAN_SPLIT_KEY, anyClientRunning.get() && eventProcessor.isStreaming())
             .with(CAN_MERGE_KEY, canMerge)
             .with(TRACKERS_LIST_KEY, trackers());
    }

    private List<Printable> trackers() {
        return segments().stream()
                         .sorted(Comparator.comparingInt(EventProcessorSegment::id))
                         .map(StreamingProcessorSegment::new)
                         .collect(Collectors.toList());
    }

    @Nonnull
    private List<EventProcessorSegment> segments() {
        List<EventProcessorSegment> segments = new LinkedList<>();
        eventProcessor.instances().forEach(instance -> instance.claimedSegments().forEach(segments::add));
        return segments;
    }
}
