/*
 *  Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
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
import io.axoniq.axonserver.util.DaemonThreadFactory;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Tags;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
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
            Executors.newScheduledThreadPool(1, new DaemonThreadFactory("index-manager-"));
    protected final Supplier<StorageProperties> storageProperties;
    protected final String context;
    private final EventType eventType;
    private final ConcurrentNavigableMap<Long, Map<String, IndexEntries>> activeIndexes = new ConcurrentSkipListMap<>();
    private final ConcurrentNavigableMap<FileVersion, PersistedBloomFilter> bloomFilterPerSegment = new ConcurrentSkipListMap<>();
    private final ConcurrentSkipListMap<FileVersion, StandardIndex> indexMap = new ConcurrentSkipListMap<>();
    private final ConcurrentNavigableMap<Long, Integer> indexesDescending = new ConcurrentSkipListMap<>(Comparator.reverseOrder());
    private final MeterFactory.RateMeter indexOpenMeter;
    private final MeterFactory.RateMeter indexCloseMeter;
    private final RemoteAggregateSequenceNumberResolver remoteIndexManager;
    private final AtomicLong useMmapAfterIndex = new AtomicLong();
    private final Counter bloomFilterOpenMeter;
    private final Counter bloomFilterCloseMeter;
    private ScheduledFuture<?> cleanupTask;
    private final String storagePath;

    private final Supplier<IndexManager> next;

    /**
     * @param context           the context of the storage engine
     * @param storageProperties storage engine configuration
     * @param eventType         content type of the event store (events or snapshots)
     * @param meterFactory      factory to create metrics meter
     */
    public StandardIndexManager(String context, Supplier<StorageProperties> storageProperties, String storagePath,
                                EventType eventType,
                                MeterFactory meterFactory) {
        this(context, storageProperties, storagePath, eventType, null, meterFactory, () -> null);
    }

    /**
     * @param context           the context of the storage engine
     * @param storageProperties storage engine configuration
     * @param eventType         content type of the event store (events or snapshots)
     * @param meterFactory      factory to create metrics meter
     */
    public StandardIndexManager(String context, Supplier<StorageProperties> storageProperties, String storagePath,
                                EventType eventType,
                                MeterFactory meterFactory, Supplier<IndexManager> next) {
        this(context, storageProperties, storagePath, eventType, null, meterFactory, next);
    }

    /**
     * @param context            the context of the storage engine
     * @param storageProperties  storage engine configuration
     * @param eventType          content type of the event store (events or snapshots)
     * @param remoteIndexManager component that provides last sequence number for old aggregates
     * @param meterFactory       factory to create metrics meter
     */
    public StandardIndexManager(String context, Supplier<StorageProperties> storageProperties, String storagePath,
                                EventType eventType,
                                RemoteAggregateSequenceNumberResolver remoteIndexManager,
                                MeterFactory meterFactory,
                                Supplier<IndexManager> next) {
        this.storageProperties = storageProperties;
        this.storagePath = storagePath;
        this.context = context;
        this.eventType = eventType;
        this.remoteIndexManager = remoteIndexManager;
        Tags tags = Tags.of(MeterFactory.CONTEXT, context, "type", eventType.name());
        this.indexOpenMeter = meterFactory.rateMeter(BaseMetricName.AXON_INDEX_OPEN, tags);
        this.indexCloseMeter = meterFactory.rateMeter(BaseMetricName.AXON_INDEX_CLOSE, tags);
        this.bloomFilterOpenMeter = meterFactory.counter(BaseMetricName.AXON_BLOOM_OPEN, tags);
        this.bloomFilterCloseMeter = meterFactory.counter(BaseMetricName.AXON_BLOOM_CLOSE, tags);
        scheduledExecutorService.scheduleAtFixedRate(this::indexCleanup, 10, 10, TimeUnit.SECONDS);
        this.next = next;
    }

    /**
     * Initializes the index manager.
     */
    public void init() {
        StorageProperties properties = storageProperties.get();
        String[] indexFiles = FileUtils.getFilesWithSuffix(new File(storagePath),
                                                           properties.getIndexSuffix());
        for (String indexFile : indexFiles) {
            FileVersion fileVersion = FileUtils.process(indexFile);
            if (properties.dataFile(storagePath, fileVersion).exists()) {
                indexesDescending.compute(fileVersion.segment(),
                                          (s, old) -> old == null ? fileVersion.segmentVersion() : Math.max(
                                                  fileVersion.segmentVersion(), old));
            } else {
                remove(fileVersion);
            }
        }

        updateUseMmapAfterIndex();
    }

    private void updateUseMmapAfterIndex() {
        useMmapAfterIndex.set(indexesDescending.keySet().stream().skip(storageProperties.get().getMaxIndexesInMemory())
                                               .findFirst()
                                               .orElse(-1L));
    }

    @Override
    public void createIndex(FileVersion segment, Map<String, List<IndexEntry>> indexEntries) {
        Map<String, IndexEntries> positionsPerAggregate = new HashMap<>();
        indexEntries.forEach((aggregateId, entries) ->
                                     positionsPerAggregate
                                             .computeIfAbsent(aggregateId,
                                                              a -> new StandardIndexEntries(
                                                                      entries.get(0).getSequenceNumber()))
                                             .addAll(entries));
        createStandardIndex(segment, positionsPerAggregate);
    }

    public void createStandardIndex(FileVersion segment, Map<String, IndexEntries> positionsPerAggregate) {
        if (mySegment(segment)) {
            doCreateIndex(segment, positionsPerAggregate);
        } else {
            StandardIndexManager nextStandardIndexManager = (StandardIndexManager) next.get();
            if (nextStandardIndexManager != null) {
                nextStandardIndexManager.createStandardIndex(segment, positionsPerAggregate);
            }
        }
    }

    private boolean mySegment(FileVersion segment) {
        return storageProperties.get().dataFile(storagePath, segment).exists();
    }

    private void doCreateIndex(FileVersion segment, Map<String, IndexEntries> positionsPerAggregate) {
        StorageProperties properties = storageProperties.get();
        if (positionsPerAggregate == null) {
            positionsPerAggregate = Collections.emptyMap();
        }
        File tempFile = properties.indexTemp(storagePath, segment.segment()); //TODO restore original temporary file
        if (!FileUtils.delete(tempFile)) {
            throw new MessagingPlatformException(ErrorCode.INDEX_WRITE_ERROR,
                                                 "Failed to delete temp index file:" + tempFile);
        }
        DBMaker.Maker maker = DBMaker.fileDB(tempFile);
        if (properties.isUseMmapIndex()) {
            maker.fileMmapEnable();
            if (properties.isForceCleanMmapIndex()) {
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
            Files.move(tempFile.toPath(), properties.index(storagePath, segment).toPath(),
                       StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new MessagingPlatformException(ErrorCode.INDEX_WRITE_ERROR,
                                                 "Failed to rename index file" + properties
                                                         .index(storagePath, segment),
                                                 e);
        }

        PersistedBloomFilter filter = new PersistedBloomFilter(properties.bloomFilter(storagePath, segment)
                                                                         .getAbsolutePath(),
                                                               positionsPerAggregate.keySet().size(),
                                                               properties.getBloomIndexFpp());
        filter.create();
        filter.insertAll(positionsPerAggregate.keySet());
        filter.store();
        bloomFilterPerSegment.put(segment, filter);
        indexesDescending.put(segment.segment(), segment.segmentVersion());
        getIndex(segment);
    }

    private IndexEntries getPositions(FileVersion fileVersion, String aggregateId) {
        if (notInBloomIndex(fileVersion, aggregateId)) {
            return null;
        }

        RuntimeException lastError = new RuntimeException();
        for (int retry = 0; retry < 3; retry++) {
            try {
                StandardIndex idx = getIndex(fileVersion);
                return idx.getPositions(aggregateId);
            } catch (IndexNotFoundException ex) {
                return null;
            } catch (Exception ex) {
                lastError = new RuntimeException(
                        "Error happened while trying get positions for " + fileVersion.segment() + " segment.", ex);
            }
        }
        throw lastError;
    }

    private StandardIndex getIndex(FileVersion fileVersion) {
        try {
            return indexMap.computeIfAbsent(fileVersion, StandardIndex::new).ensureReady();
        } catch (IndexNotFoundException indexNotFoundException) {
            StandardIndex remove = indexMap.remove(fileVersion);
            if (remove != null) {
                remove.close();
            }
            throw indexNotFoundException;
        }
    }

    private void indexCleanup() {
        StorageProperties properties = storageProperties.get();
        while (indexMap.size() > properties.getMaxIndexesInMemory()) {
            Map.Entry<FileVersion, StandardIndex> entry = indexMap.pollFirstEntry(); //TODO
            logger.debug("{}: Closing index {}", context, entry.getKey());
            cleanupTask = scheduledExecutorService.schedule(() -> entry.getValue().close(), 2, TimeUnit.SECONDS);
        }

        while (bloomFilterPerSegment.size() > properties.getMaxBloomFiltersInMemory()) {
            Map.Entry<FileVersion, PersistedBloomFilter> removed = bloomFilterPerSegment.pollFirstEntry();
            logger.debug("{}: Removed bloom filter for {} from memory", context, removed.getKey());
            bloomFilterCloseMeter.increment();
        }
    }

    private boolean notInBloomIndex(FileVersion fileVersion, String aggregateId) {
        PersistedBloomFilter persistedBloomFilter = bloomFilterPerSegment.computeIfAbsent(fileVersion,
                                                                                          this::loadBloomFilter);
        return persistedBloomFilter != null && !persistedBloomFilter.mightContain(aggregateId);
    }

    private PersistedBloomFilter loadBloomFilter(FileVersion fileVersion) {
        logger.debug("{}: open bloom filter for {}", context, fileVersion.segment());
        StorageProperties properties = storageProperties.get();
        PersistedBloomFilter filter = new PersistedBloomFilter(properties.bloomFilter(storagePath, fileVersion)
                                                                         .getAbsolutePath(), 0, 0.03f);
        if (!filter.fileExists()) {
            return null;
        }
        bloomFilterOpenMeter.increment();
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
        if (indexesDescending.containsKey(segment)) {
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
        if (indexesDescending.containsKey(segment)) {
            throw new IndexNotFoundException(segment + ": already completed");
        }

        indexEntries.forEach((aggregateId, entries) ->
                                     activeIndexes.computeIfAbsent(segment, s -> new ConcurrentHashMap<>())
                                                  .computeIfAbsent(aggregateId,
                                                                   a -> new StandardIndexEntries(
                                                                           entries.get(0).getSequenceNumber()))
                                                  .addAll(entries));
    }

    @Override
    public Mono<Void> activateVersion(long segment, int segmentVersion) {
        FileVersion fileVersion = new FileVersion(segment, segmentVersion);
        return Mono.fromSupplier(() -> storageProperties.get().index(storagePath, fileVersion))
                   .filter(indexFile -> !indexFile.exists())
                   .flatMap(indexFile -> Mono.fromSupplier(() -> storageProperties.get().transformedIndex(storagePath,
                                                                                                          fileVersion))
                                             .filter(File::exists)
                                             .switchIfEmpty(Mono.error(new RuntimeException())) //TODO custom exception
                                             .flatMap(tempIndex -> FileUtils.rename(tempIndex, indexFile)))

                   .doOnSuccess(v -> indexesDescending.put(segment, segmentVersion));
    }

    @Override
    public void createNewVersion(long segment, int version, Map<String, List<IndexEntry>> indexEntriesMap) {
        FileVersion newVersion = new FileVersion(segment, version);
        if (indexEntriesMap == null) {
            indexEntriesMap = Collections.emptyMap();
        }
        File tempFile = storageProperties.get().transformedIndex(storagePath, newVersion);
        if (!FileUtils.delete(tempFile)) {
            throw new MessagingPlatformException(ErrorCode.INDEX_WRITE_ERROR,
                                                 "Failed to delete temp index file:" + tempFile);
        }
        DBMaker.Maker maker = DBMaker.fileDB(tempFile);
        if (storageProperties.get().isUseMmapIndex()) {
            maker.fileMmapEnable();
            if (storageProperties.get().isForceCleanMmapIndex()) {
                maker.cleanerHackEnable();
            }
        } else {
            maker.fileChannelEnable();
        }
        DB db = maker.make();
        try (HTreeMap<String, IndexEntries> map = db.hashMap(AGGREGATE_MAP, Serializer.STRING,
                                                             StandardIndexEntriesSerializer.get())
                                                    .createOrOpen()) {
            indexEntriesMap.forEach((key, value) -> {
                IndexEntry first = value.get(0);
                Integer[] positions = new Integer[value.size()];
                for (int i = 0; i < value.size(); i++) {
                    positions[i] = value.get(i).getPosition();
                }
                map.put(key, new StandardIndexEntries(first.getSequenceNumber(),
                                                      positions));
            });
        }
        db.close();
        PersistedBloomFilter filter = new PersistedBloomFilter(storageProperties.get()
                                                                                .bloomFilter(storagePath, newVersion)
                                                                                .getAbsolutePath(),
                                                               indexEntriesMap.keySet().size(),
                                                               storageProperties.get().getBloomIndexFpp());
        filter.create();
        filter.insertAll(indexEntriesMap.keySet());
        filter.store();

        //    indexesDescending.put(segment, version);
    }

    @Override
    public Stream<AggregateIndexEntries> latestSequenceNumbers(FileVersion segment) {
        return getIndex(segment).latestSequenceNumbers();
    }

    @Override
    public SortedMap<FileVersion, IndexEntries> lookupAggregateInClosedSegments(String aggregateId,
                                                                                long firstSequenceNumber,
                                                                                long lastSequenceNumber,
                                                                                long maxResults,
                                                                                long minToken,
                                                                                long minTokenInPreviousSegment) {
        SortedMap<FileVersion, IndexEntries> results = new TreeMap<>();
        for (Map.Entry<Long, Integer> index : indexesDescending.entrySet()) {
            if (minTokenInPreviousSegment < minToken) {
                return results;
            }
            FileVersion fileVersion = new FileVersion(index.getKey(), index.getValue());
            IndexEntries entries = getPositions(fileVersion, aggregateId);
            logger.debug("{}: lookupAggregate {} in segment {} found {}", context, aggregateId, index, entries);
            if (entries != null) {
                int nrOfEntries = addToResult(firstSequenceNumber, lastSequenceNumber, results, fileVersion, entries);
                maxResults -= nrOfEntries;
                if (allEntriesFound(firstSequenceNumber, maxResults, entries)) {
                    return results;
                }
            }
            minTokenInPreviousSegment = index.getKey();
        }


        IndexManager nextIndexManager = next.get();
        if (nextIndexManager != null) {
            results.putAll(nextIndexManager.lookupAggregateInClosedSegments(aggregateId,
                                                                            firstSequenceNumber,
                                                                            lastSequenceNumber,
                                                                            maxResults,
                                                                            minToken,
                                                                            minTokenInPreviousSegment));
        }
        return results;
    }

    /**
     * Commpletes an active index.
     *
     * @param segment the first token in the segment
     */
    @Override
    public void complete(FileVersion segment) {
        doCreateIndex(segment, activeIndexes.get(segment.segment()));
        activeIndexes.remove(segment.segment());
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
        for (Map.Entry<Long, Integer> segment : indexesDescending.entrySet()) {
            if (checked >= maxSegments) {
                return Optional.empty();
            }
            if (segment.getKey() <= maxTokenHint) {
                IndexEntries indexEntries = getPositions(new FileVersion(segment.getKey(), segment.getValue()),
                                                         aggregateId);
                if (indexEntries != null) {
                    return Optional.of(indexEntries.lastSequenceNumber());
                }
                checked++;
            }
        }

        IndexManager nextIndexManager = next.get();
        Optional<Long> result = Optional.empty();
        if (nextIndexManager != null) {
            result = next.get().getLastSequenceNumber(aggregateId, maxSegments - checked, maxTokenHint);
        }


        if (result.isEmpty() && remoteIndexManager != null && checked < maxSegments && !indexesDescending.isEmpty()) {
            result = remoteIndexManager.getLastSequenceNumber(context,
                                                              aggregateId,
                                                              maxSegments - checked,
                                                              indexesDescending.keySet().last() - 1);
        }
        return result;
    }

    /**
     * Returns the position of the last event for an aggregate.
     *
     * @param aggregateId       the aggregate identifier
     * @param maxSequenceNumber maximum sequence number of the event to find (exclusive)
     * @return
     */
    @Override
    public SegmentIndexEntries lastIndexEntries(String aggregateId, long maxSequenceNumber) {
        for (Long segment : activeIndexes.descendingKeySet()) {
            IndexEntries indexEntries = activeIndexes.get(segment).get(aggregateId);
            if (indexEntries != null && indexEntries.firstSequenceNumber() < maxSequenceNumber) {
                return new SegmentIndexEntries(new FileVersion(segment, 0),
                                               indexEntries.range(indexEntries.firstSequenceNumber(),
                                                                  maxSequenceNumber,
                                                                  EventType.SNAPSHOT.equals(eventType)));
            }
        }

        if (!indexesDescending.isEmpty()) {
            return lastIndexEntriesFromClosedSegments(aggregateId,
                                                      maxSequenceNumber,
                                                      indexesDescending.keySet().first());
        }
        return null;
    }

    @Override
    public SegmentIndexEntries lastIndexEntriesFromClosedSegments(String aggregateId, long maxSequenceNumber,
                                                                  long startAtToken) {
        for (Map.Entry<Long, Integer> segment : indexesDescending.entrySet()) {
            FileVersion fileVersion = new FileVersion(segment.getKey(), segment.getValue());
            IndexEntries indexEntries = getPositions(fileVersion, aggregateId);
            if (indexEntries != null && indexEntries.firstSequenceNumber() < maxSequenceNumber) {
                return new SegmentIndexEntries(fileVersion, indexEntries.range(indexEntries.firstSequenceNumber(),
                                                                               maxSequenceNumber,
                                                                               EventType.SNAPSHOT.equals(eventType)));
            }
        }
        IndexManager nextIndexManager = next.get();
        if (nextIndexManager != null) {
            return nextIndexManager.lastIndexEntriesFromClosedSegments(aggregateId,
                                                                       maxSequenceNumber,
                                                                       indexesDescending.keySet().last() - 1);
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
    public boolean validIndex(FileVersion segment) {
        try {
            if (indexesDescending.containsKey(segment.segment())
                    && indexesDescending.get(segment.segment()) == segment.segmentVersion()) {
                return loadBloomFilter(segment) != null && getIndex(segment) != null;
            }
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
    public boolean remove(long segment) {
        StorageProperties properties = storageProperties.get();
        if (activeIndexes.remove(segment) == null) {
            Integer version = indexesDescending.remove(segment);
            if (version != null) {
                FileVersion fileVersion = new FileVersion(segment, version);
                StandardIndex index = indexMap.remove(fileVersion); //TODO
                if (index != null) {
                    index.close();
                }
                bloomFilterPerSegment.remove(fileVersion);
            }
        }
        return FileUtils.delete(properties.index(storagePath, segment)) &&
                FileUtils.delete(properties.bloomFilter(storagePath, segment));
    }

    @Override
    public boolean remove(FileVersion fileVersion) {
        StandardIndex index = indexMap.remove(fileVersion); //TODO
        if (index != null) {
            index.close();
        }
        bloomFilterPerSegment.remove(fileVersion);
        return FileUtils.delete(storageProperties.get().index(storagePath, fileVersion)) &&
                FileUtils.delete(storageProperties.get().bloomFilter(storagePath, fileVersion));
    }


    @Override
    public List<File> indexFiles(FileVersion segment) {
        return Arrays.asList(
                storageProperties.get().index(storagePath, segment),
                storageProperties.get().bloomFilter(storagePath, segment));
    }

    @Override
    public void addExistingIndex(FileVersion segment) {
        indexesDescending.put(segment.segment(), segment.segmentVersion());
        updateUseMmapAfterIndex();
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
    public SortedMap<FileVersion, IndexEntries> lookupAggregate(String aggregateId, long firstSequenceNumber,
                                                                long lastSequenceNumber, long maxResults,
                                                                long minToken) {
        SortedMap<FileVersion, IndexEntries> results = new TreeMap<>();
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
                int nrOfEntries = addToResult(firstSequenceNumber,
                                              lastSequenceNumber,
                                              results,
                                              new FileVersion(segment, 0),
                                              entries);
                maxResults -= nrOfEntries;
                if (allEntriesFound(firstSequenceNumber, maxResults, entries)) {
                    return results;
                }
            }
            minTokenInPreviousSegment = segment;
        }

        results.putAll(lookupAggregateInClosedSegments(aggregateId,
                                                       firstSequenceNumber,
                                                       lastSequenceNumber,
                                                       maxResults,
                                                       minToken,
                                                       minTokenInPreviousSegment));
        return results;
    }

    private int addToResult(long firstSequenceNumber, long lastSequenceNumber,
                            SortedMap<FileVersion, IndexEntries> results, FileVersion segment, IndexEntries entries) {
        entries = entries.range(firstSequenceNumber, lastSequenceNumber, EventType.SNAPSHOT.equals(eventType));
        if (!entries.isEmpty()) {
            results.put(segment, entries);
        }
        return entries.size();
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
        indexMap.clear(); //TODO
        indexesDescending.clear();
        if (cleanupTask != null && !cleanupTask.isDone()) {
            cleanupTask.cancel(true);
        }
        IndexManager nextIndexManager = next.get();
        if (nextIndexManager != null) {
            nextIndexManager.cleanup(delete);
        }
    }

    @Override
    public Stream<File> getBackupFilenames(long lastSegmentBackedUp, int lastVersionBackedUp) {
        StorageProperties properties = storageProperties.get();
        return indexesDescending.entrySet()
                                .stream()
                                .filter(s -> s.getKey() > lastSegmentBackedUp || s.getValue() > lastVersionBackedUp)
                                .flatMap(s -> Stream.of(
                                        properties.index(storagePath, s.getKey()),
                                        properties.bloomFilter(storagePath, s.getKey())
                                ));
    }

    private class StandardIndex implements Closeable {

        private final FileVersion segment;
        private final Object initLock = new Object();
        private volatile boolean initialized;
        private HTreeMap<String, IndexEntries> positions;
        private DB db;


        private StandardIndex(FileVersion fileVersion) {
            this.segment = fileVersion;
        }

        public IndexEntries getPositions(String aggregateId) {
            return positions.get(aggregateId);
        }

        @Override
        public void close() {
            if (logger.isDebugEnabled()) {
                logger.debug("{}: close {}", segment, storageProperties.get().index(storagePath, segment));
            }
            if (db != null && !db.isClosed()) {
                indexCloseMeter.mark();
                positions.close();
                db.close();
            }
        }

        public StandardIndex ensureReady() {
            if (initialized && !db.isClosed()) {
                return this;
            }

            synchronized (initLock) {
                if (initialized && !db.isClosed()) {
                    return this;
                }

                StorageProperties properties = storageProperties.get();
                if (!properties.index(storagePath, segment).exists()) {
                    throw new IndexNotFoundException("Index not found for segment: " + segment);
                }
                indexOpenMeter.mark();
                logger.debug("{}: open {}", segment, properties.index(storagePath, segment));
                DBMaker.Maker maker = DBMaker.fileDB(properties.index(storagePath, segment))
                                             .readOnly()
                                             .fileLockDisable();
                if (properties.isUseMmapIndex() && segment.segment() > useMmapAfterIndex.get()) {
                    maker.fileMmapEnable();
                    if (properties.isForceCleanMmapIndex()) {
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

        public Stream<AggregateIndexEntries> latestSequenceNumbers() {
            return positions.entrySet().stream().map(e -> new AggregateIndexEntries(e.getKey(), e.getValue()));
        }
    }
}
