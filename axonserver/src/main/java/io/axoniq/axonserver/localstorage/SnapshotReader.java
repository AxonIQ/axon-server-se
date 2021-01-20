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
public class SnapshotReader {
    private final EventStorageEngine datafileManagerChain;

    public SnapshotReader(EventStorageEngine datafileManagerChain) {
        this.datafileManagerChain = datafileManagerChain;
    }

    public Optional<SerializedEvent> readSnapshot(String aggregateId, long minSequenceNumber) {
            return datafileManagerChain
                    .getLastEvent(aggregateId, minSequenceNumber)
                    .map(SerializedEvent::asSnapshot);
    }

    public void streamByAggregateId(String aggregateId, long minSequenceNumber, long maxSequenceNumber, int maxResults, Consumer<SerializedEvent> eventConsumer) {
        datafileManagerChain.processEventsPerAggregateHighestFirst(aggregateId,
                                                                   minSequenceNumber,
                                                                   maxSequenceNumber,
                                                                   maxResults,
                                                                   e -> eventConsumer.accept(e.asSnapshot()));
    }
}
