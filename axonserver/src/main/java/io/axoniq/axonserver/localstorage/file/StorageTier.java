/*
 *  Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axonserver.grpc.event.EventWithToken;
import io.axoniq.axonserver.localstorage.QueryOptions;
import io.axoniq.axonserver.localstorage.SerializedEvent;
import reactor.core.publisher.Flux;

import java.io.File;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.SortedSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

/**
 * Interface StorageTier that defines the contract for a storage tier.
 *
 * @author Stefan Dragisic
 * @since 2023.0.0
 */
public interface StorageTier {
    /**
     * Retrieve events for positions in a segment.
     *
     * @param segment the segment to retrieve events from
     * @param indexEntries the positions of events to retrieve
     * @param prefetch the number of events to prefetch
     * @return the events for the given positions
     */
    Flux<SerializedEvent> eventsForPositions(FileVersion segment, IndexEntries indexEntries, int prefetch);

    /**
     * Queries events from storage.
     *
     * @param queryOptions the options to use for querying events
     * @param consumer the consumer to which the events will be delivered
     */
    void query(QueryOptions queryOptions, Predicate<EventWithToken> consumer);

    /**
     * Reads a single serialized event from storage.
     *
     * @param minSequenceNumber the minimum sequence number for the event
     * @param maxSequenceNumber the maximum sequence number for the event
     * @param lastEventPosition the last event position
     * @return the serialized event or an empty optional if no event was found
     */
    Optional<SerializedEvent> readSerializedEvent(long minSequenceNumber, long maxSequenceNumber, SegmentIndexEntries lastEventPosition);

    /**
     * Get the set of segments in storage.
     *
     * @return the set of segments
     */
    SortedSet<Long> getSegments();

    /**
     * Get the first token in storage.
     *
     * @return the first token
     */
    long getFirstToken();

    /**
     * Get the token for a given instant.
     *
     * @param instant the instant for which to retrieve the token
     * @return the token
     */
    long getTokenAt(long instant);

    /**
     * Get events from a given segment.
     *
     * @param segment the segment to retrieve events from
     * @param token the token to start retrieval from
     * @return the event iterator
     */
    EventIterator getEvents(long segment, long token);

    /**
     * Get transactions from a given segment.
     *
     * @param segment the segment to retrieve transactions from
     * @param token the token to start retrieval from
     * @param validating whether the transactions should be validated
     * @return the transaction iterator
     */
    TransactionIterator getTransactions(long segment, long token, boolean validating);


    /**
     * Get the segment for a given token.
     *
     * @param token the token for which to retrieve the segment
     * @return the segment for the given token
     */
    long getSegmentFor(long token);

/**
 * Retrieve events for an aggregate from storage.
 *
 * @param segment the segment to retrieve events from
 * @param indexEntries the positions of events to retrieve
 * @param minSequenceNumber the minimum sequence number for the events
 * @param maxSequenceNumber the maximum sequence number for the events
 * @param onEvent the consumer to which the events will be delivered
 * @param maxResults the maximum number of events to retrieve
 * @param minToken the minimum token
 */
    int retrieveEventsForAnAggregate(FileVersion segment, List<Integer> indexEntries, long minSequenceNumber,
                                     long maxSequenceNumber,
                                     Consumer<SerializedEvent> onEvent, long maxResults, long minToken);

    Stream<String> getBackupFilenames(long lastSegmentBackedUp, int lastVersionBackedUp, boolean includeActive);

    Optional<EventSource> eventSource(FileVersion segment);

    void close(boolean deleteData);

    void initSegments(long first);

    long getFirstCompletedSegment();

    void handover(Segment segment, Runnable callback);

    int nextVersion();

    boolean removeSegment(long segment, int segmentVersion);

    Integer currentSegmentVersion(Long segment);

    void activateSegmentVersion(long segment, int segmentVersion);

    SortedSet<FileVersion> segmentsWithoutIndex();

    /**
     * Represents the abstract RetentionStrategy class.
     */
    abstract class RetentionStrategy {

        protected final Iterator<Long> segmentIterator;
        protected final Map<String, String> contextMetaData;
        protected final String retentionProperty;

        protected final NavigableMap<Long, Integer> segments;

        protected final Function<FileVersion, File> dataFileResolver;

        /**
         * Constructs a RetentionStrategy instance.
         * @param propertyName the name of the retention property
         * @param contextMetaData the context metadata map
         * @param segments the NavigableMap of segments
         * @param dataFileResolver the data file resolver function
         */
        RetentionStrategy(String propertyName,
                          Map<String, String> contextMetaData,
                          NavigableMap<Long, Integer> segments,
                          Function<FileVersion, File> dataFileResolver) {
            this.retentionProperty = propertyName;
            this.contextMetaData = contextMetaData;
            this.segments = segments;
            this.segmentIterator = segments.descendingKeySet().iterator();
            this.dataFileResolver = dataFileResolver;

        }

        /**
         * Abstract method to find the next segment.
         * @return Long
         */
        public abstract Long findNextSegment();

    }

    /**
     * Represents the TimeBasedRetentionStrategy class.
     */
    class TimeBasedRetentionStrategy extends RetentionStrategy {

        private final Long retentionTimeInMillis = retentionWeight();

        /**
         * Constructs a TimeBasedRetentionStrategy instance.
         * @param propertyName the name of the retention property
         * @param contextMetaData the context metadata map
         * @param segments the NavigableMap of segments
         * @param dataFileResolver the data file resolver function
         */
        public TimeBasedRetentionStrategy(String propertyName,
                                          Map<String, String> contextMetaData,
                                          NavigableMap<Long, Integer> segments,
                                          Function<FileVersion, File> dataFileResolver) {
            super(propertyName, contextMetaData, segments, dataFileResolver);
        }

        /**
         * Overrides the findNextSegment method of the super class to find the next segment using time-based retention strategy.
         * @return Long
         */
        @Override
        public Long findNextSegment() {
            if (segmentIterator.hasNext()) {
                long minTimestamp = System.currentTimeMillis() - retentionTimeInMillis;
                long candidate = segmentIterator.next();


                if (segments.firstKey() == candidate) {
                    return null;
                }


                Map.Entry<Long, Integer> firstEntry = segments.firstEntry();
                if (dataFileResolver.apply(new FileVersion(firstEntry.getKey(), firstEntry.getValue())).lastModified() < minTimestamp) {
                    return candidate;
                }
            }
            return null;
        }

        /**
         * Method to find the retention weight in milliseconds.
         * @return Long
         */
        public Long retentionWeight() {
            String retentionTimeString = contextMetaData.get(retentionProperty);
            if (retentionTimeString == null) {
                return null;
            }
            return Duration.parse(retentionTimeString).toMillis();
        }
    }

    /**
     * Represents the SizeBasedRetentionStrategy class.
     */
    class SizeBasedRetentionStrategy extends RetentionStrategy {

        private final Long retentionSizeInBytes = retentionWeight();
        private final AtomicLong currentTotalSize;

        /**
         * Constructs a SizeBasedRetentionStrategy instance.
         * @param propertyName the name of the retention property
         * @param contextMetaData the context metadata map
         * @param segments the NavigableMap of segments
         * @param dataFileResolver the data file resolver function
         */
        public SizeBasedRetentionStrategy(String propertyName,
                                          Map<String, String> contextMetaData,
                                          NavigableMap<Long, Integer> segments,
                                          Function<FileVersion, File> dataFileResolver
        ) {
            super(propertyName, contextMetaData, segments, dataFileResolver);
            this.currentTotalSize = getTotalSize(segments);
        }

        @Nonnull
        private AtomicLong getTotalSize(NavigableMap<Long, Integer> segments) {
            AtomicLong total = new AtomicLong();
            segments.forEach((segment, version) -> total.addAndGet(dataFileResolver.apply(new FileVersion(segment, version)).length()));
            return total;
        }

        /**
         * Overrides the findNextSegment method of the super class to find the next segment using size-based retention strategy.
         * @return Long
         */
        @Override
        public Long findNextSegment() {
            if (segmentIterator.hasNext()) {
                long totalSize = currentTotalSize.get();
                if (totalSize >= retentionSizeInBytes) {
                    Long nextSegment = segmentIterator.next();
                    currentTotalSize.addAndGet(-dataFileResolver.apply(new FileVersion(nextSegment, segments.get(nextSegment))).length());
                    return nextSegment;
                }
            }
            return null;
        }

        /**
         * Method to find the retention weight in bytes.
         * @return Long
         */
        public Long retentionWeight() {
            String retentionSizeString = contextMetaData.get(retentionProperty);
            if (retentionSizeString == null) {
                return null;
            }

            return Long.parseLong(retentionSizeString);
        }
    }
}
