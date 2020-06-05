/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.file;

import java.util.Optional;
import java.util.SortedMap;

/**
 * @author Marc Gathier
 */
public interface IndexManager {

    void addToActiveSegment(long segment, String aggregateId, IndexEntry indexEntry);

    void complete(long segment);

    Optional<Long> getLastSequenceNumber(String aggregateId, int maxSegments);

    IndexEntries positions(long segment, String aggregateId);

    boolean validIndex(long segment);

    void remove(long segment);

    SortedMap<Long, IndexEntries> lookupAggregate(String aggregateId, long firstSequenceNumber, long lastSequenceNumber,
                                                  long maxResults, boolean snapshot);

    void cleanup(boolean delete);

    void init();

    SegmentAndPosition lastEvent(String aggregateId, long minSequenceNumber);
}
