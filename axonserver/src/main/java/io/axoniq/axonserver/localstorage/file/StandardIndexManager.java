/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.localstorage.EventType;
import io.axoniq.axonserver.metric.BaseMetricName;
import io.axoniq.axonserver.metric.MeterFactory;
import io.micrometer.core.instrument.Tags;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/**
 * Implementation of the index manager that creates 2 files per segment, an index file containing a map of aggregate
 * identifiers and the position of events for this aggregate and a bloom filter to quickly check if an aggregate occurs
 * in the segment.
 *
 * @author Marc Gathier
 * @since 4.4
 */
public class StandardIndexManager implements IndexManager {

    private static final Logger logger = LoggerFactory.getLogger(StandardIndexManager.class);
    private static final String AGGREGATE_MAP = "aggregateMap";
    private static final ScheduledExecutorService scheduledExecutorService =
            Executors.newScheduledThreadPool(1, new CustomizableThreadFactory("index-manager-"));
    protected final StorageProperties storageProperties;
    protected final String context;
    private final EventType eventType;
    private final ConcurrentNavigableMap<Long, Map<String, IndexEntries>> activeIndexes = new ConcurrentSkipListMap<>();
    private final ConcurrentNavigableMap<Long, PersistedBloomFilter> bloomFilterPerSegment = new ConcurrentSkipListMap<>();
    private final ConcurrentSkipListMap<Long, Index> indexMap = new ConcurrentSkipListMap<>();
    private final SortedSet<Long> indexesDescending = new ConcurrentSkipListSet<>(Comparator.reverseOrder());
    private final MeterFactory.RateMeter indexOpenMeter;
    private final MeterFactory.RateMeter indexCloseMeter;
    private final RemoteAggregateSequenceNumberResolver remoteIndexManager;
    private ScheduledFuture<?> cleanupTask;
    private final AtomicLong useMmapAfterIndex = new AtomicLong();

    /**
     * @param context           the context of the storage engine
     * @param storageProperties storage engine configuration
     * @param eventType         content type of the event store (events or snapshots)
     * @param meterFactory      factory to create metrics meter
     */
    public StandardIndexManager(String context, StorageProperties storageProperties, EventType eventType,
                                MeterFactory meterFactory) {
        this(context, storageProperties, eventType, null, meterFactory);
    }

    /**
     * @param context            the context of the storage engine
     * @param storageProperties  storage engine configuration
     * @param eventType          content type of the event store (events or snapshots)
     * @param remoteIndexManager component that provides last sequence number for old aggregates
     * @param meterFactory       factory to create metrics meter
     */
    public StandardIndexManager(String context, StorageProperties storageProperties, EventType eventType,
                                RemoteAggregateSequenceNumberResolver remoteIndexManager,
                                MeterFactory meterFactory) {
        this.storageProperties = storageProperties;
        this.context = context;
        this.eventType = eventType;
        this.remoteIndexManager = remoteIndexManager;
        this.indexOpenMeter = meterFactory.rateMeter(BaseMetricName.AXON_INDEX_OPEN,
                                                     Tags.of(MeterFactory.CONTEXT, context));
        this.indexCloseMeter = meterFactory.rateMeter(BaseMetricName.AXON_INDEX_CLOSE,
                                                      Tags.of(MeterFactory.CONTEXT, context));
        scheduledExecutorService.scheduleAtFixedRate(this::indexCleanup, 10, 10, TimeUnit.SECONDS);
    }

    /**
     * Initializes the index manager.
     */
    public void init() {
        String[] indexFiles = FileUtils.getFilesWithSuffix(new File(storageProperties.getStorage(context)),
                                                           storageProperties.getIndexSuffix());
        for (String indexFile : indexFiles) {
            long index = Long.parseLong(indexFile.substring(0, indexFile.indexOf('.')));
            indexesDescending.add(index);
        }

        updateUseMmapAfterIndex();
    }

    private void updateUseMmapAfterIndex() {
        useMmapAfterIndex.set(indexesDescending.stream().skip(storageProperties.getMaxIndexesInMemory()).findFirst()
                                               .orElse(-1L));
    }

    private void createIndex(Long segment, Map<String, IndexEntries> positionsPerAggregate) {
        if (positionsPerAggregate == null) {
            positionsPerAggregate = Collections.emptyMap();
        }
        File tempFile = storageProperties.indexTemp(context, segment);
        if (!FileUtils.delete(tempFile)) {
            throw new MessagingPlatformException(ErrorCode.INDEX_WRITE_ERROR,
                                                 "Failed to delete temp index file:" + tempFile);
        }
        DBMaker.Maker maker = DBMaker.fileDB(tempFile);
        if (storageProperties.isUseMmapIndex()) {
            maker.fileMmapEnable();
            if (storageProperties.isForceCleanMmapIndex()) {
                maker.cleanerHackEnable();
            }
        } else {
            maker.fileChannelEnable();
        }
        DB db = maker.make();
        try (HTreeMap<String, IndexEntries> map = db.hashMap(AGGREGATE_MAP, Serializer.STRING,
                                                             StandardIndexEntriesSerializer.get())
                                                    .createOrOpen()) {
            map.putAll(positionsPerAggregate);
        }
        db.close();

        try {
            Files.move(tempFile.toPath(), storageProperties.index(context, segment).toPath(),
                       StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new MessagingPlatformException(ErrorCode.INDEX_WRITE_ERROR,
                                                 "Failed to rename index file" + storageProperties
                                                         .index(context, segment),
                                                 e);
        }

        PersistedBloomFilter filter = new PersistedBloomFilter(storageProperties.bloomFilter(context, segment)
                                                                                .getAbsolutePath(),
                                                               positionsPerAggregate.keySet().size(),
                                                               storageProperties.getBloomIndexFpp());
        filter.create();
        filter.insertAll(positionsPerAggregate.keySet());
        filter.store();
        bloomFilterPerSegment.put(segment, filter);

        getIndex(segment);
    }

    private IndexEntries getPositions(long segment, String aggregateId) {
        if (notInBloomIndex(segment, aggregateId)) {
            return null;
        }

        RuntimeException lastError = new RuntimeException();
        for (int retry = 0; retry < 3; retry++) {
            try {
                Index idx = getIndex(segment);
                return idx.getPositions(aggregateId);
            } catch (IndexNotFoundException ex) {
                return null;
            } catch (Exception ex) {
                lastError = new RuntimeException(
                        "Error happened while trying get positions for " + segment + " segment.", ex);
            }
        }
        throw lastError;
    }

    private Index getIndex(long segment) {
        try {
            return indexMap.computeIfAbsent(segment, Index::new).ensureReady();
        } catch (IndexNotFoundException indexNotFoundException) {
            indexMap.remove(segment);
            throw indexNotFoundException;
        }
    }

    private void indexCleanup() {
        while (indexMap.size() > storageProperties.getMaxIndexesInMemory()) {
            Map.Entry<Long, Index> entry = indexMap.pollFirstEntry();
            logger.debug("{}: Closing index {}", context, entry.getKey());
            cleanupTask = scheduledExecutorService.schedule(() -> entry.getValue().close(), 2, TimeUnit.SECONDS);
        }

        while (bloomFilterPerSegment.size() > storageProperties.getMaxBloomFiltersInMemory()) {
            Map.Entry<Long, PersistedBloomFilter> removed = bloomFilterPerSegment.pollFirstEntry();
            logger.debug("{}: Removed bloomfilter for {} from memory", context, removed.getKey());
        }
    }

    private boolean notInBloomIndex(Long segment, String aggregateId) {
        PersistedBloomFilter persistedBloomFilter = bloomFilterPerSegment.computeIfAbsent(segment,
                                                                                          i -> loadBloomFilter(segment));
        return persistedBloomFilter != null && !persistedBloomFilter.mightContain(aggregateId);
    }

    private PersistedBloomFilter loadBloomFilter(Long segment) {
        logger.debug("{}: open bloom filter for {}", context, segment);
        PersistedBloomFilter filter = new PersistedBloomFilter(storageProperties.bloomFilter(context, segment)
                                                                                .getAbsolutePath(), 0, 0.03f);
        if (!filter.fileExists()) {
            return null;
        }
        filter.load();
        return filter;
    }

    /**
     * Adds the position of an event for an aggregate to an active (writable) index.
     *
     * @param segment     the segment number
     * @param aggregateId the identifier for the aggregate
     * @param indexEntry  position, sequence number and token of the new entry
     */
    @Override
    public void addToActiveSegment(long segment, String aggregateId, IndexEntry indexEntry) {
        if (indexesDescending.contains(segment)) {
            throw new IndexNotFoundException(segment + ": already completed");
        }
        activeIndexes.computeIfAbsent(segment, s -> new ConcurrentHashMap<>())
                     .computeIfAbsent(aggregateId, a -> new StandardIndexEntries(indexEntry.getSequenceNumber()))
                     .add(indexEntry);
    }

    /**
     * Adds positions of a number of events for aggregates to an active (writable) index.
     *
     * @param segment      the segment number
     * @param indexEntries the new entries to add
     */
    @Override
    public void addToActiveSegment(Long segment, Map<String, List<IndexEntry>> indexEntries) {
        if (indexesDescending.contains(segment)) {
            throw new IndexNotFoundException(segment + ": already completed");
        }

        indexEntries.forEach((aggregateId, entries) ->
                                     activeIndexes.computeIfAbsent(segment, s -> new ConcurrentHashMap<>())
                                                  .computeIfAbsent(aggregateId,
                                                                   a -> new StandardIndexEntries(
                                                                           entries.get(0).getSequenceNumber()))
                                                  .addAll(entries));
    }

    /**
     * Commpletes an active index.
     *
     * @param segment the first token in the segment
     */
    @Override
    public void complete(long segment) {
        createIndex(segment, activeIndexes.get(segment));
        indexesDescending.add(segment);
        activeIndexes.remove(segment);
        updateUseMmapAfterIndex();
    }

    /**
     * Returns the last sequence number of an aggregate if this is found.
     *
     * @param aggregateId  the identifier for the aggregate
     * @param maxSegments  maximum number of segments to check for the aggregate
     * @param maxTokenHint maximum token to check
     * @return last sequence number for the aggregate (if found)
     */
    @Override
    public Optional<Long> getLastSequenceNumber(String aggregateId, int maxSegments, long maxTokenHint) {
        if (activeIndexes.isEmpty()) {
            return Optional.empty();
        }
        int checked = 0;
        for (Long segment : activeIndexes.descendingKeySet()) {
            if (checked >= maxSegments) {
                return Optional.empty();
            }
            if (segment <= maxTokenHint) {
                IndexEntries indexEntries = activeIndexes.get(segment).get(aggregateId);
                if (indexEntries != null) {
                    return Optional.of(indexEntries.lastSequenceNumber());
                }
                checked++;
            }
        }
        for (Long segment : indexesDescending) {
            if (checked >= maxSegments) {
                return Optional.empty();
            }
            if (segment <= maxTokenHint) {
                IndexEntries indexEntries = getPositions(segment, aggregateId);
                if (indexEntries != null) {
                    return Optional.of(indexEntries.lastSequenceNumber());
                }
                checked++;
            }
        }
        if (remoteIndexManager != null && checked < maxSegments) {
            return remoteIndexManager.getLastSequenceNumber(context,
                                                            aggregateId,
                                                            maxSegments - checked,
                                                            indexesDescending.isEmpty() ?
                                                                    activeIndexes.firstKey() - 1 :
                                                                    indexesDescending.last() - 1);
        }
        return Optional.empty();
    }

    /**
     * Returns the position of the last event for an aggregate.
     *
     * @param aggregateId       the aggregate identifier
     * @param minSequenceNumber minimum sequence number of the event to find
     * @return
     */
    @Override
    public SegmentAndPosition lastEvent(String aggregateId, long minSequenceNumber) {
        for (Long segment : activeIndexes.descendingKeySet()) {
            IndexEntries indexEntries = activeIndexes.get(segment).get(aggregateId);
            if (indexEntries != null) {
                if (minSequenceNumber < indexEntries.lastSequenceNumber()) {
                    return new SegmentAndPosition(segment, indexEntries.last());
                } else {
                    return null;
                }
            }
        }
        for (Long segment : indexesDescending) {
            IndexEntries indexEntries = getPositions(segment, aggregateId);
            if (indexEntries != null) {
                if (minSequenceNumber < indexEntries.lastSequenceNumber()) {
                    return new SegmentAndPosition(segment, indexEntries.last());
                } else {
                    return null;
                }
            }
        }
        return null;
    }

    /**
     * Checks if the index and bloom filter for the segment exist.
     *
     * @param segment the segment number
     * @return true if the index for this segment is valid
     */
    @Override
    public boolean validIndex(long segment) {
        try {
            return loadBloomFilter(segment) != null && getIndex(segment) != null;
        } catch (Exception ex) {
            logger.warn("Failed to validate index for segment: {}", segment, ex);
        }
        return false;
    }

    /**
     * Removes the index and bloom filter for the segment
     *
     * @param segment the segment number
     */
    @Override
    public boolean remove(long segment) {
        if (activeIndexes.remove(segment) == null) {
            Index index = indexMap.remove(segment);
            if (index != null) {
                index.close();
            }
            bloomFilterPerSegment.remove(segment);
            indexesDescending.remove(segment);
        }
        return FileUtils.delete(storageProperties.index(context, segment)) &&
                FileUtils.delete(storageProperties.bloomFilter(context, segment));
    }

    /**
     * Finds all positions for an aggregate within the specified sequence number range.
     *
     * @param aggregateId         the aggregate identifier
     * @param firstSequenceNumber minimum sequence number for the events returned (inclusive)
     * @param lastSequenceNumber  maximum sequence number for the events returned (exclusive)
     * @param maxResults          maximum number of results allowed
     * @return all positions for an aggregate within the specified sequence number range
     */
    @Override
    public SortedMap<Long, IndexEntries> lookupAggregate(String aggregateId, long firstSequenceNumber,
                                                         long lastSequenceNumber, long maxResults, long minToken) {
        SortedMap<Long, IndexEntries> results = new TreeMap<>();
        logger.debug("{}: lookupAggregate {} minSequenceNumber {}, lastSequenceNumber {}",
                     context,
                     aggregateId,
                     firstSequenceNumber,
                     lastSequenceNumber);

        long minTokenInPreviousSegment = Long.MAX_VALUE;
        for (Long segment : activeIndexes.descendingKeySet()) {
            if (minTokenInPreviousSegment < minToken) {
                return results;
            }
            IndexEntries entries = activeIndexes.getOrDefault(segment, Collections.emptyMap()).get(aggregateId);
            if (entries != null) {
                entries = addToResult(firstSequenceNumber, lastSequenceNumber, results, segment, entries);
                maxResults -= entries.size();
                if (allEntriesFound(firstSequenceNumber, maxResults, entries)) {
                    return results;
                }
            }
            minTokenInPreviousSegment = segment;
        }

        for (Long index : indexesDescending) {
            if (minTokenInPreviousSegment < minToken) {
                return results;
            }
            IndexEntries entries = getPositions(index, aggregateId);
            logger.debug("{}: lookupAggregate {} in segment {} found {}", context, aggregateId, index, entries);
            if (entries != null) {
                entries = addToResult(firstSequenceNumber, lastSequenceNumber, results, index, entries);
                maxResults -= entries.size();
                if (allEntriesFound(firstSequenceNumber, maxResults, entries)) {
                    return results;
                }
            }
            minTokenInPreviousSegment = index;
        }

        return results;
    }

    private IndexEntries addToResult(long firstSequenceNumber, long lastSequenceNumber,
                                     SortedMap<Long, IndexEntries> results, Long segment, IndexEntries entries) {
        entries = entries.range(firstSequenceNumber, lastSequenceNumber, EventType.SNAPSHOT.equals(eventType));
        if (!entries.isEmpty()) {
            results.put(segment, entries);
        }
        return entries;
    }

    private boolean allEntriesFound(long firstSequenceNumber, long maxResults, IndexEntries entries) {
        return !entries.isEmpty() && firstSequenceNumber >= entries.firstSequenceNumber() || maxResults <= 0;
    }

    /**
     * Cleanup the index manager.
     *
     * @param delete flag to indicate that all indexes should be deleted
     */
    public void cleanup(boolean delete) {
        activeIndexes.clear();
        bloomFilterPerSegment.clear();
        indexMap.forEach((segment, index) -> index.close());
        indexMap.clear();
        indexesDescending.clear();
        if (cleanupTask != null && !cleanupTask.isDone()) {
            cleanupTask.cancel(true);
        }
    }

    @Override
    public Stream<String> getBackupFilenames(long lastSegmentBackedUp) {
        return indexesDescending.stream()
                                .filter(s -> s > lastSegmentBackedUp)
                                .flatMap(s -> Stream.of(
                                        storageProperties.index(context, s).getAbsolutePath(),
                                        storageProperties.bloomFilter(context, s).getAbsolutePath()
                                ));
    }

    private class Index implements Closeable {

        private final long segment;
        private final Object initLock = new Object();
        private volatile boolean initialized;
        private HTreeMap<String, IndexEntries> positions;
        private DB db;


        private Index(long segment) {
            this.segment = segment;
        }

        public IndexEntries getPositions(String aggregateId) {
            return positions.get(aggregateId);
        }

        @Override
        public void close() {
            logger.debug("{}: close {}", segment, storageProperties.index(context, segment));
            if (db != null && !db.isClosed()) {
                indexCloseMeter.mark();
                positions.close();
                db.close();
            }
        }

        public Index ensureReady() {
            if (initialized && !db.isClosed()) {
                return this;
            }

            synchronized (initLock) {
                if (initialized && !db.isClosed()) {
                    return this;
                }

                if (!storageProperties.index(context, segment).exists()) {
                    throw new IndexNotFoundException("Index not found for segment: " + segment);
                }
                indexOpenMeter.mark();
                logger.debug("{}: open {}", segment, storageProperties.index(context, segment));
                DBMaker.Maker maker = DBMaker.fileDB(storageProperties.index(context, segment))
                                             .readOnly()
                                             .fileLockDisable();
                if (storageProperties.isUseMmapIndex() && segment > useMmapAfterIndex.get()) {
                    maker.fileMmapEnable();
                    if (storageProperties.isForceCleanMmapIndex()) {
                        maker.cleanerHackEnable();
                    }
                } else {
                    maker.fileChannelEnable();
                }
                this.db = maker.make();
                this.positions = db.hashMap(AGGREGATE_MAP, Serializer.STRING, StandardIndexEntriesSerializer.get())
                                   .createOrOpen();
                initialized = true;
            }
            return this;
        }
    }
}
