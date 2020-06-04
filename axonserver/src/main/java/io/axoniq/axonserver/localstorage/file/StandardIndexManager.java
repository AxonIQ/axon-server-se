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
import java.util.Collections;
import java.util.Comparator;
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

/**
 * @author Marc Gathier
 */
public class StandardIndexManager implements IndexManager {

    private static final Logger logger = LoggerFactory.getLogger(StandardIndexManager.class);
    private static final String AGGREGATE_MAP = "aggregateMap";
    private static final ScheduledExecutorService scheduledExecutorService =
            Executors.newScheduledThreadPool(1, new CustomizableThreadFactory("index-manager-"));
    protected final StorageProperties storageProperties;
    protected final String context;
    private final ConcurrentNavigableMap<Long, Map<String, IndexEntries>> activeIndexes = new ConcurrentSkipListMap<>();
    private final ConcurrentNavigableMap<Long, PersistedBloomFilter> bloomFilterPerSegment = new ConcurrentSkipListMap<>();
    private final ConcurrentSkipListMap<Long, Index> indexMap = new ConcurrentSkipListMap<>();
    private final SortedSet<Long> indexes = new ConcurrentSkipListSet<>(Comparator.reverseOrder());
    private final MeterFactory.RateMeter indexOpenMeter;
    private final MeterFactory.RateMeter indexCloseMeter;
    private ScheduledFuture<?> cleanupTask;

    public StandardIndexManager(String context, StorageProperties storageProperties,
                                MeterFactory meterFactory) {
        this.storageProperties = storageProperties;
        this.context = context;
        this.indexOpenMeter = meterFactory.rateMeter(BaseMetricName.AXON_INDEX_OPEN,
                                                     Tags.of(MeterFactory.CONTEXT, context));
        this.indexCloseMeter = meterFactory.rateMeter(BaseMetricName.AXON_INDEX_CLOSE,
                                                      Tags.of(MeterFactory.CONTEXT, context));
        scheduledExecutorService.scheduleAtFixedRate(this::indexCleanup, 10, 10, TimeUnit.SECONDS);
    }

    public void init() {
        String[] indexFiles = FileUtils.getFilesWithSuffix(new File(storageProperties.getStorage(context)),
                                                           storageProperties.getIndexSuffix());
        for (String indexFile : indexFiles) {
            long index = Long.parseLong(indexFile.substring(0, indexFile.indexOf('.')));
            indexes.add(index);
        }
    }

    private void createIndex(Long segment, Map<String, IndexEntries> positionsPerAggregate) {
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

        if (!tempFile.renameTo(storageProperties.index(context, segment))) {
            throw new MessagingPlatformException(ErrorCode.INDEX_WRITE_ERROR,
                                                 "Failed to rename index file:" + tempFile);
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
            } catch (Throwable ex) {
                lastError = new RuntimeException(
                        "Error happened while trying get positions for " + segment + " segment.", ex);
            }
        }
        throw lastError;
    }

    /**
     * Returns the {@link Index} for the specified segment.
     *
     * @param segment the segment
     * @return the {@link Index} for the specified segment
     *
     * @throws IndexNotFoundException if the attempt to open tha index file fails
     */
    public Index getIndex(long segment) {
        return indexMap.computeIfAbsent(segment, Index::new).ensureReady();
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

    @Override
    public void addToActiveSegment(long segment, String aggregateId, IndexEntry indexEntry) {
        activeIndexes.computeIfAbsent(segment, s -> new ConcurrentHashMap<>())
                     .computeIfAbsent(aggregateId, a -> new StandardIndexEntries(indexEntry.getSequenceNumber()))
                     .add(indexEntry);
    }

    @Override
    public void complete(long segment) {
        createIndex(segment, activeIndexes.get(segment));
        activeIndexes.remove(segment);
        indexes.add(segment);
    }

    @Override
    public Optional<Long> getLastSequenceNumber(String aggregateId, int maxSegments) {
        int checked = 0;
        for (Long segment : activeIndexes.descendingKeySet()) {
            if (checked >= maxSegments) {
                return Optional.empty();
            }
            IndexEntries indexEntries = activeIndexes.get(segment).get(aggregateId);
            if (indexEntries != null) {
                return Optional.of(indexEntries.lastSequenceNumber());
            }
            checked++;
        }
        for (Long segment : indexes) {
            if (checked >= maxSegments) {
                return Optional.empty();
            }
            IndexEntries indexEntries = getPositions(segment, aggregateId);
            if (indexEntries != null) {
                return Optional.of(indexEntries.lastSequenceNumber());
            }
            checked++;
        }
        return Optional.empty();
    }

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
        for (Long segment : indexes) {
            IndexEntries indexEntries = getPositions(segment, aggregateId);
            if (indexEntries != null) {
                if (minSequenceNumber > indexEntries.lastSequenceNumber()) {
                    return new SegmentAndPosition(segment, indexEntries.last());
                } else {
                    return null;
                }
            }
        }
        return null;
    }

    @Override
    public IndexEntries positions(long segment, String aggregateId) {
        logger.debug("{}: positions for {} in  segment {}.{}",
                     context,
                     aggregateId,
                     segment,
                     storageProperties.getIndexSuffix());
        if (activeIndexes.containsKey(segment)) {
            return activeIndexes.get(segment).get(aggregateId);
        }
        return getPositions(segment, aggregateId);
    }

    public boolean validIndex(long segment) {
        try {
            return loadBloomFilter(segment) != null && getIndex(segment) != null;
        } catch (Exception ex) {
            logger.warn("Failed to validate index for segment: {}", segment, ex);
        }
        return false;
    }

    @Override
    public void remove(long segment) {
        if (activeIndexes.remove(segment) == null) {
            Index index = indexMap.remove(segment);
            if (index != null) {
                index.close();
            }
            bloomFilterPerSegment.remove(segment);
            indexes.remove(segment);
            FileUtils.delete(storageProperties.index(context, segment));
            FileUtils.delete(storageProperties.bloomFilter(context, segment));
        }
    }

    @Override
    public SortedMap<Long, IndexEntries> lookupAggregate(String aggregateId, long firstSequenceNumber,
                                                         long lastSequenceNumber, long maxResults) {
        SortedMap<Long, IndexEntries> results = new TreeMap<>();
        logger.debug("{}: lookupAggregate {} minSequenceNumber {}, lastSequenceNumber {}",
                     context,
                     aggregateId,
                     firstSequenceNumber,
                     lastSequenceNumber);

        for (Long segment : activeIndexes.descendingKeySet()) {
            logger.debug("{}: lookupAggregate {} in segment {} map size {}",
                         context,
                         aggregateId,
                         segment,
                         activeIndexes.getOrDefault(segment, Collections
                                 .emptyMap()).size());

            IndexEntries entries = activeIndexes.getOrDefault(segment, Collections.emptyMap()).get(aggregateId);
            logger.debug("{}: lookupAggregate {} in segment {} found {}", context, aggregateId, segment, entries);
            if (entries != null) {
                IndexEntries range = entries.range(firstSequenceNumber, lastSequenceNumber);
                if (!range.isEmpty()) {
                    results.put(segment, range);
                    maxResults -= range.size();
                    if (firstSequenceNumber >= entries.firstSequenceNumber() || maxResults <= 0) {
                        return results;
                    }
                }
            }
        }

        for (Long index : indexes) {
            logger.debug("{}: lookupAggregate {} in segment {}.{}",
                         context,
                         aggregateId,
                         index,
                         storageProperties.getIndexSuffix());
            IndexEntries entries = getPositions(index, aggregateId);
            logger.debug("{}: lookupAggregate {} in segment {} found {}", context, aggregateId, index, entries);
            if (entries != null) {
                IndexEntries range = entries.range(firstSequenceNumber, lastSequenceNumber);
                if (!range.isEmpty()) {
                    results.put(index, range);
                    if (firstSequenceNumber >= entries.firstSequenceNumber()) {
                        return results;
                    }
                }
            }
        }

        return results;
    }

    public void cleanup(boolean delete) {
        activeIndexes.clear();
        bloomFilterPerSegment.clear();
        indexMap.forEach((segment, index) -> index.close());
        indexMap.clear();
        if (cleanupTask != null && !cleanupTask.isDone()) {
            cleanupTask.cancel(true);
        }
    }

    public class Index implements Closeable {

        private final long segment;
        private final Object initLock = new Object();
        private volatile boolean initialized;
        private Map<String, IndexEntries> positions;
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
                if (storageProperties.isUseMmapIndex()) {
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
