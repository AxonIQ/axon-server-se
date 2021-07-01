/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Optional;
import java.util.function.Consumer;

/**
 * @author Marc Gathier
 */
public class AggregateReader {

    private final EventStorageEngine eventStorageEngine;
    private final SnapshotReader snapshotReader;

    public AggregateReader(EventStorageEngine eventStorageEngine, SnapshotReader snapshotReader) {
        this.eventStorageEngine = eventStorageEngine;
        this.snapshotReader = snapshotReader;
    }

    public void readEvents(String aggregateId, boolean useSnapshots, long minSequenceNumber,
                           Consumer<SerializedEvent> eventConsumer) {
        readEvents(aggregateId, useSnapshots, minSequenceNumber, Long.MAX_VALUE, 0, eventConsumer);
    }

    public void readEvents(String aggregateId, boolean useSnapshots, long minSequenceNumber, long maxSequenceNumber,
                           long minToken,
                           Consumer<SerializedEvent> eventConsumer) {
        long actualMinSequenceNumber = minSequenceNumber;
        if (useSnapshots) {
            Optional<SerializedEvent> snapshot = snapshotReader.readSnapshot(aggregateId,
                                                                             minSequenceNumber,
                                                                             maxSequenceNumber);
            if (snapshot.isPresent()) {
                eventConsumer.accept(snapshot.get());
                actualMinSequenceNumber = snapshot.get().asEvent().getAggregateSequenceNumber() + 1;
            }
        }
        eventStorageEngine.processEventsPerAggregate(aggregateId, actualMinSequenceNumber, maxSequenceNumber, minToken, eventConsumer);

    }

    public void readSnapshots(String aggregateId, long minSequenceNumber, long maxSequenceNumber, int maxResults,
                              Consumer<SerializedEvent> eventConsumer) {
        snapshotReader.streamByAggregateId(aggregateId,
                                           minSequenceNumber,
                                           maxSequenceNumber,
                                           maxResults > 0 ? maxResults : Integer.MAX_VALUE,
                                           e -> eventConsumer.accept(e.asSnapshot()));
    }

    public long readHighestSequenceNr(String aggregateId) {
        return eventStorageEngine.getLastSequenceNumber(aggregateId).orElse(-1L);
    }

    public long readHighestSequenceNr(String aggregateId, int maxSegmentsHint, long maxTokenHint) {
        return eventStorageEngine.getLastSequenceNumber(aggregateId, maxSegmentsHint, maxTokenHint).orElse(-1L);
    }

    /**
     * Returns the events related to the specified aggregate, the have a sequence number included between the specified
     * boundaries, and token greater than the specified minimum token. The result may start with a snapshot event if the
     * {@code useSnapshots} parameter is {@code true} and a snapshot is present in the event store for the specified
     * aggregate.
     *
     * @param aggregateId       the identifier of the aggregate
     * @param useSnapshots      if true, the returned events could start from a snapshot, if present
     * @param minSequenceNumber the minimum sequence number of the events that are returned
     * @param maxSequenceNumber the maximum sequence number of the events that are returned (exclusive)
     * @param minTokenHint      used for performance reason to avoid to look for events with lower token than this value
     * @return the events related to the specific aggregate
     */
    public Flux<SerializedEvent> events(String aggregateId,
                                        boolean useSnapshots,
                                        long minSequenceNumber,
                                        long maxSequenceNumber,
                                        long minTokenHint) {

        return snapshot(aggregateId, useSnapshots, minSequenceNumber, maxSequenceNumber)
                .flatMapMany(snapshot ->
                        Flux.just(snapshot).concatWith(eventStorageEngine.eventsPerAggregate(aggregateId,
                                snapshot.getAggregateSequenceNumber()+1,
                                maxSequenceNumber,
                                minTokenHint)))
                .switchIfEmpty(eventStorageEngine.eventsPerAggregate(aggregateId,
                        minSequenceNumber,
                        maxSequenceNumber,
                        minTokenHint));
    }


    private Mono<SerializedEvent> snapshot(String aggregateId,
                                   boolean useSnapshots,
                                   long minSequenceNumber,
                                   long maxSequenceNumber){
        if (useSnapshots){
            return snapshotReader.snapshot(aggregateId, minSequenceNumber, maxSequenceNumber)
            .filter(snapshot -> snapshot.getAggregateSequenceNumber() < maxSequenceNumber);
        } else {
            return Mono.empty();
        }

    }
}
