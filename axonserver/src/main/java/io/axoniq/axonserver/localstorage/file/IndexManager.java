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
 * Manages index for an event store. There are two IndexManagers per context, one for the events and one for the
 * snapshots.
 *
 * @author Marc Gathier
 * @since 4.4
 */
public interface IndexManager {

    /**
     * Initializes the index manager.
     */
    void init();

    /**
     * Adds a new index entry to an active segment.
     *
     * @param segment     the segment number
     * @param aggregateId the identifier for the aggregate
     * @param indexEntry  position, sequence number and token of the new entry
     */
    void addToActiveSegment(long segment, String aggregateId, IndexEntry indexEntry);

    /**
     * Completes an active segment.
     *
     * @param segment the first token in the segment
     */
    void complete(long segment);

    /**
     * Retrieves the sequence number of the last event for the given aggregate.
     *
     * @param aggregateId  the identifier for the aggregate
     * @param maxSegments  maximum number of segments to check for the aggregate
     * @param maxTokenHint maximum token to check for events of this aggregate
     * @return the sequence number of the last event for the given aggregate
     */
    Optional<Long> getLastSequenceNumber(String aggregateId, int maxSegments, long maxTokenHint);

    /**
     * Validates that the index for the given segment exists.
     *
     * @param segment the segment number
     */
    boolean validIndex(long segment);

    /**
     * Removes index entries for a specific segment.
     *
     * @param segment the segment number
     */
    void remove(long segment);

    /**
     * Finds all locations of events for the given aggregate within range of sequence numbers specified.
     *
     * @param aggregateId         the aggregate identifier
     * @param firstSequenceNumber minimum sequence number for the events returned (inclusive)
     * @param lastSequenceNumber  maximum sequence number for the events returned (exclusive)
     * @param maxResults          maximum number of results allowed
     * @return map of positions per segment
     */
    SortedMap<Long, IndexEntries> lookupAggregate(String aggregateId, long firstSequenceNumber, long lastSequenceNumber,
                                                  long maxResults);

    /**
     * Stops index manager and optionally deletes all indexes.
     *
     * @param delete flag to indicate that all indexes should be deleted
     */
    void cleanup(boolean delete);


    /**
     * Retrieves the last segment and position of an event for given aggregate. Returns null if aggregate is not
     * found, or no event with sequence number greater than or equal to the minimum sequence number.
     *
     * @param aggregateId       the aggregate identifier
     * @param minSequenceNumber minimum sequence number of the event to find
     * @return segment and position of the last event
     */
    SegmentAndPosition lastEvent(String aggregateId, long minSequenceNumber);
}
