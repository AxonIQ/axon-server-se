/*
 *  Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.file;

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

    Stream<String> getBackupFilenames(long lastSegmentBackedUp, int lastVersionBackedUp, boolean includeActive);

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

    void close(boolean deleteData);

    void initSegments(long first);

    void handover(Segment segment, Runnable callback);

    boolean removeSegment(long segment, int segmentVersion);

    Integer currentSegmentVersion(Long segment);

    void activateSegmentVersion(long segment, int segmentVersion);

    SortedSet<FileVersion> segmentsWithoutIndex();

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
