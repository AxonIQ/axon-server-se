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
import java.util.NavigableSet;
import java.util.Optional;
import java.util.SortedSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

/**
 * @author Stefan Dragisic
 */
public interface StorageTier {
    Flux<SerializedEvent> eventsForPositions(long segment, IndexEntries indexEntries, int prefetch);

    void query(QueryOptions queryOptions, Predicate<EventWithToken> consumer);

    Optional<SerializedEvent> readSerializedEvent(long minSequenceNumber, long maxSequenceNumber, SegmentIndexEntries lastEventPosition);

    SortedSet<Long> getSegments();

    long getFirstToken();

    long getTokenAt(long instant);

    EventIterator getEvents(long segment, long token);

    TransactionIterator getTransactions(long segment, long token, boolean validating);

    long getSegmentFor(long token);

    int retrieveEventsForAnAggregate(long segment, List<Integer> indexEntries, long minSequenceNumber,
                                     long maxSequenceNumber,
                                     Consumer<SerializedEvent> onEvent, long maxResults, long minToken);

    Stream<String> getBackupFilenames(long lastSegmentBackedUp, boolean includeActive);

    Optional<EventSource> eventSource(long segment);

    void close(boolean deleteData);

    void initSegments(long first);

    long getFirstCompletedSegment();

    void handover(Segment segment, Runnable callback);

    abstract class RetentionStrategy {

        protected final Iterator<Long> segmentIterator;
        protected final Map<String, String> contextMetaData;
        protected final String retentionProperty;

        protected final SortedSet<Long> segments;

        protected final Function<Long, File> dataFileResolver;

        RetentionStrategy(String propertyName,
                          Map<String, String> contextMetaData,
                          NavigableSet<Long> segments,
                          Function<Long, File> dataFileResolver) {
            this.retentionProperty = propertyName;
            this.contextMetaData = contextMetaData;
            this.segments = segments;
            this.segmentIterator = segments.descendingIterator();
            this.dataFileResolver = dataFileResolver;

        }

        public abstract Long findNextSegment();

    }


    class TimeBasedRetentionStrategy extends RetentionStrategy {

        private final Long retentionTimeInMillis = retentionWeight();

        public TimeBasedRetentionStrategy(String propertyName,
                                          Map<String, String> contextMetaData,
                                          NavigableSet<Long> segments,
                                          Function<Long, File> dataFileResolver) {
            super(propertyName, contextMetaData, segments, dataFileResolver);
        }

        @Override
        public Long findNextSegment() {
            if (segmentIterator.hasNext()) {
                long minTimestamp = System.currentTimeMillis() - retentionTimeInMillis;
                long candidate = segmentIterator.next();


                if (segments.first() == candidate) {
                    return null;
                }


                if (dataFileResolver.apply(candidate).lastModified() < minTimestamp) {
                    return candidate;
                }
            }
            return null;
        }

        public Long retentionWeight() {
            String retentionTimeString = contextMetaData.get(retentionProperty);
            if (retentionTimeString == null) {
                return null;
            }
            return Duration.parse(retentionTimeString).toMillis();
        }
    }

    class SizeBasedRetentionStrategy extends RetentionStrategy {

        private final Long retentionSizeInBytes = retentionWeight();
        private final AtomicLong currentTotalSize;

        public SizeBasedRetentionStrategy(String propertyName,
                                          Map<String, String> contextMetaData,
                                          NavigableSet<Long> segments,
                                          Function<Long, File> dataFileResolver
        ) {
            super(propertyName, contextMetaData, segments, dataFileResolver);
            this.currentTotalSize = getTotalSize(segments);
        }

        @Nonnull
        private AtomicLong getTotalSize(NavigableSet<Long> segments) {
            AtomicLong total = new AtomicLong();
            segments.forEach(segment -> total.addAndGet(dataFileResolver.apply(segment).length()));
            return total;
        }

        @Override
        public Long findNextSegment() {
            if (segmentIterator.hasNext()) {
                long totalSize = currentTotalSize.get();
                if (totalSize >= retentionSizeInBytes) {
                    Long nextSegment = segmentIterator.next();
                    currentTotalSize.addAndGet(-dataFileResolver.apply(nextSegment).length());
                    return nextSegment;
                }
            }
            return null;
        }

        public Long retentionWeight() {
            String retentionSizeString = contextMetaData.get(retentionProperty);
            if (retentionSizeString == null) {
                return null;
            }

            return Long.parseLong(retentionSizeString);
        }
    }
}
