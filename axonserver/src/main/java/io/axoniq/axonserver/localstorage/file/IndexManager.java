/*
 *  Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.file;

import reactor.core.publisher.Mono;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

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
    default void complete(long segment) {
        complete(new FileVersion(segment, 0));
    }

    void complete(FileVersion segment);
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
    boolean validIndex(FileVersion segment);

    /**
     * Removes index entries for a specific segment.
     *
     * @param segment the segment number
     * @return true if all index related files for the segment have been removed
     */
    boolean remove(long segment);

    /**
     * Returns index files for segment
     *
     * @param segment
     * @return list of index files
     */
    List<File> indexFiles(FileVersion segment);

    /**
     * Add existing, non-active, index file to index manager
     *
     * @param segment to add
     */
    void addExistingIndex(FileVersion segment);

    /**
     * Finds all locations of events for the given aggregate within range of sequence numbers specified.
     *
     * @param aggregateId         the aggregate identifier
     * @param firstSequenceNumber minimum sequence number for the events returned (inclusive)
     * @param lastSequenceNumber  maximum sequence number for the events returned (exclusive)
     * @param maxResults          maximum number of results allowed
     * @param minToken            minimum token hint for the entries to return
     * @return map of positions per segment
     */
    SortedMap<FileVersion, IndexEntries> lookupAggregate(String aggregateId, long firstSequenceNumber, long lastSequenceNumber,
                                                  long maxResults, long minToken);

    @Nonnull
    SortedMap<FileVersion, IndexEntries> lookupAggregateInClosedSegments(String aggregateId, long firstSequenceNumber,
                                                                  long lastSequenceNumber, long maxResults,
                                                                  long minToken, long previousToken);

    /**
     * Stops index manager and optionally deletes all indexes.
     *
     * @param delete flag to indicate that all indexes should be deleted
     */
    void cleanup(boolean delete);


    /**
     * Retrieves the index entries of the last segment containing the aggregate where the first sequence number of
     * events/snapshots for the aggregate in the segment is lower than {@code maxSequenceNumber}.
     *
     * @param aggregateId       the aggregate identifier
     * @param maxSequenceNumber maximum sequence number of the event to find (exclusive)
     * @return segment and position of events for the aggregate in the given segment
     */
    SegmentIndexEntries lastIndexEntries(String aggregateId, long maxSequenceNumber);

    /**
     * Returns a stream of index related files that should be included in the backup
     *
     * @param lastSegmentBackedUp the sequence number of the last already backed up segment
     * @param lastVersionBackedUp
     * @return stream of index related files
     */
    Stream<File> getBackupFilenames(long lastSegmentBackedUp, int lastVersionBackedUp);


    /**
     * todo comment
     * @param aggregateId
     * @param maxSequenceNumber
     * @param startAtToken
     * @return
     */
    SegmentIndexEntries lastIndexEntriesFromClosedSegments(String aggregateId, long maxSequenceNumber,
                                                           long startAtToken);


    /**
     * Adds a number of index entries for a segment.
     *
     * @param segment      segment to add entries to
     * @param indexEntries list of index entries to add
     */
    void addToActiveSegment(Long segment, Map<String, List<IndexEntry>> indexEntries);

    void createNewVersion(long segment, int version, Map<String, List<IndexEntry>> indexEntriesMap);

    boolean remove(FileVersion fileVersion);

    default Mono<Void> activateVersion(FileVersion fileVersion) {
        return activateVersion(fileVersion.segment(), fileVersion.segmentVersion());
    }

    /**
     * Rename the temporary index if it exists into the active segmentVersion and active it.
     *
     */
    Mono<Void> activateVersion(long segment, int segmentVersion);

    /**
     * todo comment
     *
     * @param segment
     * @return
     */
    Stream<AggregateIndexEntries> latestSequenceNumbers(FileVersion segment);

    void createIndex(FileVersion segment, Map<String, List<IndexEntry>> entries);
}
