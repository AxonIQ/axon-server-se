/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage;

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

    public void readEvents(String aggregateId, boolean useSnapshots, long minSequenceNumber, Consumer<SerializedEvent> eventConsumer) {
        long actualMinSequenceNumber = minSequenceNumber;
        if( useSnapshots) {
            Optional<SerializedEvent> snapshot = snapshotReader.readSnapshot(aggregateId, minSequenceNumber);
            if( snapshot.isPresent()) {
                eventConsumer.accept(snapshot.get());
                actualMinSequenceNumber = snapshot.get().asEvent().getAggregateSequenceNumber() + 1;
            }
        }
        eventStorageEngine.streamByAggregateId(aggregateId, actualMinSequenceNumber, eventConsumer);

    }
    public void readSnapshots(String aggregateId, long minSequenceNumber, long maxSequenceNumber, int maxResults, Consumer<SerializedEvent> eventConsumer) {
        snapshotReader.streamByAggregateId(aggregateId, minSequenceNumber, maxSequenceNumber,
                                           maxResults > 0 ? maxResults : Integer.MAX_VALUE, eventConsumer);

    }

    public long readHighestSequenceNr(String aggregateId) {
        return eventStorageEngine.getLastSequenceNumber(aggregateId).orElse(-1L);
    }
}
