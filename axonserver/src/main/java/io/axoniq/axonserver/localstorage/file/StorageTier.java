/*
 *  Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.file;

import org.springframework.boot.convert.ApplicationConversionService;

import java.io.File;
import java.time.Duration;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.SortedSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
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
     * Get the set of segments in storage.
     *
     * @return the set of segments
     */
    SortedSet<Long> getSegments();

    Stream<String> getBackupFilenames(long lastSegmentBackedUp, int lastVersionBackedUp);

    /**
     * Retrieves an {@link EventSource} for the given segment with specific version from this StorageTier or any linked
     * storage tiers.
     *
     * @param segment the segment and version to retrieve
     * @return an EventSource for the segment, or empty if not found
     */
    Optional<EventSource> eventSource(FileVersion segment);

    /**
     * Retrieves an {@link EventSource} for the active version fo the given segment from this StorageTier or any linked
     * storage tiers.
     *
     * @param segment the segment and version to retrieve
     * @return an EventSource for the segment, or empty if not found
     */
    Optional<EventSource> eventSource(long segment);

    /**
     * Close this tier and all subsequent tiers. Deletes all files from the event store if {@code deletData} is
     * {@code true}
     *
     * @param deleteData delete the event store files
     */
    void close(boolean deleteData);

    /**
     * Initialize this storage tier and all subsequent storage tiers.
     *
     * @param lastInitialized segment number of last segment managed by the previous tier
     */
    void initSegments(long lastInitialized);


    /**
     * Receives a new segment to manage in this tier. Once this tier has completed its handover activities it invokes
     * the callback.
     *
     * @param segment  description of the segment to handover
     * @param callback a callback executed when this tier has completed the handover
     */
    void handover(Segment segment, Runnable callback);

    boolean removeSegment(long segment, int segmentVersion);

    /**
     * Retrieves the current segment version for the given segment. Forwards the request to the subsequent tier if the
     * segment is not managed by this tier.
     *
     * @param segment the segment number
     * @return the current version of the segment
     */
    Integer currentSegmentVersion(Long segment);

    /**
     * Mark the given version as the active version for the given segment. Forwards the request to the subsequent tier
     * if the segment is not managed by this tier.
     *
     * @param segment        the segment number
     * @param segmentVersion the new active segment version
     */
    void activateSegmentVersion(long segment, int segmentVersion);

    /**
     * Retries a set of segments that don't have the corresponding index files from this tier and all subsequent tiers.
     * The set is sorted in ascending order.
     *
     * @return a set of segments numbers and versions
     */
    SortedSet<FileVersion> segmentsWithoutIndex();

    /**
     * Returns a stream of segment number of segments managed by this tier and all subsequent tiers. The sequence
     * numbers are returned in descending order.
     *
     * @return a stream of segment numbers
     */
    Stream<Long> allSegments();


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

                Integer candidateVersion = segments.get(candidate);
                if (candidateVersion == null) {
                    return null;
                }
                File file = dataFileResolver.apply(new FileVersion(candidate, candidateVersion));
                if (file.lastModified() < minTimestamp) {
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
            Duration duration = ApplicationConversionService
                    .getSharedInstance().convert(retentionTimeString, Duration.class);

            return duration == null ? null : duration.toMillis();
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
