package io.axoniq.axonserver.enterprise.storage.file.xref;

import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.localstorage.EventType;
import io.axoniq.axonserver.localstorage.file.FileUtils;
import io.axoniq.axonserver.localstorage.file.IndexEntries;
import io.axoniq.axonserver.localstorage.file.IndexEntry;
import io.axoniq.axonserver.localstorage.file.IndexManager;
import io.axoniq.axonserver.localstorage.file.IndexNotFoundException;
import io.axoniq.axonserver.localstorage.file.SegmentAndPosition;
import io.axoniq.axonserver.localstorage.file.StorageProperties;
import io.axoniq.axonserver.metric.BaseMetricName;
import io.axoniq.axonserver.metric.MeterFactory;
import io.micrometer.core.instrument.Tags;
import org.jetbrains.annotations.NotNull;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

/**
 * Implementation of the {@link IndexManager} that creates an index file per segment, where for each aggregate the index
 * contains next to the positions of the events in the current segment the previous token of the aggregate. This allowed
 * read operations to immediately jump to the correct previous segment.
 * Apart from this it maintains a global index, which contains the last sequence number and token per aggregate. This
 * index is used to find the latest event for an aggregate.
 *
 * @author Marc Gathier
 * @since 4.4
 */
public class JumpSkipIndexManager implements IndexManager {

    private static final Logger logger = LoggerFactory.getLogger(JumpSkipIndexManager.class);
    private static final Serializer<JumpSkipIndexEntries> INDEX_SERIALIZER = new Serializer<JumpSkipIndexEntries>() {
        public void serialize(@Nonnull DataOutput2 dataOutput2, @Nonnull JumpSkipIndexEntries positionInfo)
                throws IOException {
            dataOutput2.packLong(positionInfo.firstSequenceNumber());
            dataOutput2.packLong(positionInfo.previousToken());
            dataOutput2.packInt(positionInfo.size());
            for (Integer position : positionInfo.positions()) {
                dataOutput2.packInt(position);
            }
        }

        public JumpSkipIndexEntries deserialize(@Nonnull DataInput2 dataInput2, int available) throws IOException {
            long firstSequenceNumber = dataInput2.unpackLong();
            long previous = dataInput2.unpackLong();
            int count = dataInput2.unpackInt();
            List<Integer> entries = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                entries.add(dataInput2.unpackInt());
            }
            return new JumpSkipIndexEntries(previous, firstSequenceNumber, entries);
        }
    };
    private static final ScheduledExecutorService scheduledExecutorService =
            Executors.newScheduledThreadPool(1, new CustomizableThreadFactory("js-index-manager-"));
    private final ConcurrentNavigableMap<Long, Map<String, JumpSkipIndexEntries>> activeIndexes = new ConcurrentSkipListMap<>();
    private final String context;
    private final NavigableMap<Long, Index> openIndexes = new ConcurrentSkipListMap<>();
    private final StorageProperties storageProperties;
    private final EventType eventType;
    private final TreeSet<Long> indexes = new TreeSet<>();
    private final String suffix;
    private final MeterFactory.RateMeter indexOpenMeter;
    private final MeterFactory.RateMeter indexCloseMeter;
    private final GlobalIndex globalIndex;
    private ScheduledFuture<?> cleanupTask;

    /**
     * @param context           the context of the storage engine
     * @param storageProperties storage engine configuration
     * @param eventType         content type of the event store (events or snapshots)
     * @param meterFactory      factory to create metrics meter
     */
    public JumpSkipIndexManager(String context, StorageProperties storageProperties, EventType eventType,
                                MeterFactory meterFactory) {
        this.context = context;
        this.storageProperties = storageProperties;
        this.eventType = eventType;
        this.indexOpenMeter = meterFactory.rateMeter(BaseMetricName.AXON_INDEX_OPEN,
                                                     Tags.of(MeterFactory.CONTEXT, context));
        this.indexCloseMeter = meterFactory.rateMeter(BaseMetricName.AXON_INDEX_CLOSE,
                                                      Tags.of(MeterFactory.CONTEXT, context));

        this.globalIndex = new GlobalIndex(storageProperties, context);

        suffix = storageProperties.getNewIndexSuffix();
        scheduledExecutorService.scheduleAtFixedRate(this::indexCleanup, 10, 10, TimeUnit.SECONDS);
    }

    /**
     * Initializes the index manager.
     */
    @Override
    public void init() {
        globalIndex.init();
        File[] indexFiles = new File(storageProperties.getStorage(context)).listFiles(n -> n.getName()
                                                                                            .endsWith(suffix));
        for (File indexFile : indexFiles) {
            long segment = Long.parseLong(indexFile.getName().substring(0, indexFile.getName().indexOf(suffix)));
            indexes.add(segment);
        }
    }


    private void indexCleanup() {
        while (openIndexes.size() > storageProperties.getMaxIndexesInMemory()) {
            Map.Entry<Long, Index> entry = openIndexes.pollFirstEntry();
            logger.debug("{}: Closing index {}", context, entry.getKey());
            cleanupTask = scheduledExecutorService.schedule(() -> entry.getValue().close(), 2, TimeUnit.SECONDS);
        }
    }

    /**
     * Adds a new position of an event to the index.
     *
     * @param segment     the segment number
     * @param aggregateId the identifier for the aggregate
     * @param indexEntry  position, sequence number and token of the new entry
     */
    @Override
    public void addToActiveSegment(long segment, String aggregateId, IndexEntry indexEntry) {
        if (indexes.contains(segment)) {
            throw new IndexNotFoundException(segment + ": already completed");
        }
        JumpSkipIndexEntries entries = activeIndexes.computeIfAbsent(segment, s -> new ConcurrentHashMap<>()).get(
                aggregateId);
        if (entries == null) {
            if (indexEntry.getSequenceNumber() > 0) {
                LastEventPositionInfo lastPositionInfo = lastActiveIndexEntry(aggregateId);
                if (lastPositionInfo != null) {
                    entries = new JumpSkipIndexEntries(lastPositionInfo.token(),
                                                       indexEntry.getSequenceNumber(),
                                                       new CopyOnWriteArrayList<>());
                } else {
                    LastEventPositionInfo entry = globalIndex.getOrDefault(aggregateId,
                                                                           new LastEventPositionInfo(-1,
                                                                                                     -1));
                    entries = new JumpSkipIndexEntries(entry.token(),
                                                       indexEntry.getSequenceNumber(),
                                                       new CopyOnWriteArrayList<>());
                }
            } else {
                entries = new JumpSkipIndexEntries(-1, indexEntry.getSequenceNumber(), new CopyOnWriteArrayList<>());
            }
            activeIndexes.get(segment).put(aggregateId, entries);
        }
        entries.add(indexEntry);
    }

    @Override
    public void addToActiveSegment(Long segment, Map<String, List<IndexEntry>> indexEntries) {
        if (indexes.contains(segment)) {
            throw new IndexNotFoundException(segment + ": already completed");
        }
        indexEntries.forEach((aggregateId, newEntries) -> {
            JumpSkipIndexEntries entries = activeIndexes.computeIfAbsent(segment, s -> new ConcurrentHashMap<>()).get(
                    aggregateId);
            if (entries == null) {
                IndexEntry indexEntry = newEntries.get(0);
                entries = initEntriesForAggregate(aggregateId, indexEntry);
                activeIndexes.get(segment).put(aggregateId, entries);
            }
            entries.addAll(newEntries);
        });
    }

    @NotNull
    private JumpSkipIndexEntries initEntriesForAggregate(String aggregateId, IndexEntry indexEntry) {
        JumpSkipIndexEntries entries;
        if (indexEntry.getSequenceNumber() > 0) {
            LastEventPositionInfo lastPositionInfo = lastActiveIndexEntry(aggregateId);
            if (lastPositionInfo != null) {
                entries = new JumpSkipIndexEntries(lastPositionInfo.token(),
                                                   indexEntry.getSequenceNumber(),
                                                   new LinkedList<>());
            } else {
                LastEventPositionInfo entry = globalIndex.getOrDefault(aggregateId,
                                                                       new LastEventPositionInfo(-1,
                                                                                                 -1));
                entries = new JumpSkipIndexEntries(entry.token(),
                                                   indexEntry.getSequenceNumber(),
                                                   new LinkedList<>());
            }
        } else {
            entries = new JumpSkipIndexEntries(-1, indexEntry.getSequenceNumber(), new CopyOnWriteArrayList<>());
        }
        return entries;
    }

    private void updateIndexes(long segment, Map<String, JumpSkipIndexEntries> updatedPositions) {
        updatedPositions = updatedPositions == null ? Collections.emptyMap() : updatedPositions;
        File tempIndex = createTempIndex(segment, updatedPositions);
        globalIndex.add(updatedPositions);

        try {
            Files.move(tempIndex.toPath(), storageProperties.newIndex(context, segment).toPath(),
                       StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new MessagingPlatformException(ErrorCode.INDEX_WRITE_ERROR,
                                                 "Failed to rename index file" + storageProperties
                                                         .index(context, segment),
                                                 e);
        }
        indexes.add(segment);
    }

    private File createTempIndex(Long segment, Map<String, JumpSkipIndexEntries> updatedPositions) {
        logger.debug("{}: create {}, keys {}",
                     segment,
                     storageProperties.newIndex(context, segment),
                     updatedPositions.keySet());
        File tempIndex = storageProperties.newIndexTemp(context, segment);
        try {
            Files.deleteIfExists(tempIndex.toPath());
        } catch (IOException ioException) {
            throw new MessagingPlatformException(ErrorCode.INDEX_WRITE_ERROR,
                                                 "Failed to delete temporary index file",
                                                 ioException);
        }
        DB index = DBMaker.fileDB(tempIndex)
                          .fileMmapEnable()
                          .cleanerHackEnable()
                          .make();

        try (HTreeMap<String, JumpSkipIndexEntries> segmentIndexMap = index.hashMap("index",
                                                                                    Serializer.STRING,
                                                                                    INDEX_SERIALIZER)
                                                                           .create()) {
            updatedPositions.forEach(segmentIndexMap::put);
        }
        index.close();
        return tempIndex;
    }

    private long segmentForToken(long token) {
        NavigableMap<Long, Map<String, JumpSkipIndexEntries>> activeSegmentsBefore = activeIndexes.headMap(token, true);
        if (!activeSegmentsBefore.isEmpty()) {
            return activeSegmentsBefore.lastKey();
        }


        NavigableSet<Long> segmentsBefore = indexes.headSet(token, true);
        if (segmentsBefore.isEmpty()) {
            throw new IndexNotFoundException(context + ":Index for token not found: " + token);
        }
        return segmentsBefore.last();
    }

    private Index getIndex(Long segment) {
        try {
            return openIndexes.computeIfAbsent(segment, Index::new).ensureReady();
        } catch (IndexNotFoundException indexNotFoundException) {
            openIndexes.remove(segment);
            throw indexNotFoundException;
        }
    }


    /**
     * Completes an active segment. Writes index file and updates the global index.
     *
     * @param segment the first token in the segment
     */
    @Override
    public void complete(long segment) {
        updateIndexes(segment, activeIndexes.get(segment));
        activeIndexes.remove(segment);
        indexes.add(segment);
    }

    /**
     * Returns the last sequence number for an aggregate.
     *
     * @param aggregateId  the identifier for the aggregate
     * @param maxSegments  maximum number of segments to check for the aggregate
     * @param maxTokenHint
     * @return the last sequence number for an aggregate
     */
    @Override
    public Optional<Long> getLastSequenceNumber(String aggregateId, int maxSegments, long maxTokenHint) {
        for (Long segment : activeIndexes.descendingKeySet()) {
            IndexEntries entries = activeIndexes.getOrDefault(segment, Collections.emptyMap()).get(aggregateId);
            if (entries != null) {
                return Optional.of(entries.lastSequenceNumber());
            }
        }
        LastEventPositionInfo positionInfo = globalIndex.get(aggregateId);
        if (positionInfo == null) {
            return Optional.empty();
        }

        return Optional.of(positionInfo.lastSequence());
    }

    private LastEventPositionInfo lastActiveIndexEntry(String aggregateId) {
        for (Long segment : activeIndexes.descendingKeySet()) {
            logger.debug("{}: lastActiveIndexEntry {} in segment {} map size {}",
                         context,
                         aggregateId,
                         segment,
                         activeIndexes.getOrDefault(segment, Collections
                                 .emptyMap()).size());

            JumpSkipIndexEntries entries = activeIndexes.getOrDefault(segment, Collections.emptyMap()).get(aggregateId);
            if (entries != null) {
                return entries.lastEventPositionInfo();
            }
        }

        return null;
    }

    /**
     * Returns the last position of an event for an aggregate.
     *
     * @param aggregateId       the aggregate identifier
     * @param minSequenceNumber minimum sequence number of the event to find
     * @return the last position of an event for an aggregate
     */
    @Override
    public SegmentAndPosition lastEvent(String aggregateId, long minSequenceNumber) {
        for (Long segment : activeIndexes.descendingKeySet()) {
            logger.debug("{}: lastEvent {} in segment {} map size {}",
                         context,
                         aggregateId,
                         segment,
                         activeIndexes.getOrDefault(segment, Collections
                                 .emptyMap()).size());

            IndexEntries entries = activeIndexes.getOrDefault(segment, Collections.emptyMap()).get(aggregateId);
            if (entries != null) {
                if (entries.lastSequenceNumber() >= minSequenceNumber) {
                    return new SegmentAndPosition(segment, entries.last());
                }
                return null;
            }
        }
        LastEventPositionInfo positionInfo = globalIndex.get(aggregateId);
        if (positionInfo == null) {
            return null;
        }
        long foundInSegment = segmentForToken(positionInfo.token());
        IndexEntries entries = getIndex(foundInSegment).get(aggregateId);
        if (entries != null && entries.lastSequenceNumber() >= minSequenceNumber) {
            return new SegmentAndPosition(foundInSegment, entries.last());
        }

        return null;
    }

    /**
     * Checks if the index file for the segment exists.
     *
     * @param segment the segment number
     * @return true if index file for the segment exists.
     */
    @Override
    public boolean validIndex(long segment) {
        boolean valid = false;
        try {
            valid = storageProperties.newIndex(context, segment).exists();

            if (!valid) {
                File tempIndex = storageProperties.newIndexTemp(context, segment);
                if (tempIndex.exists()) {
                    try (DB db = DBMaker.fileDB(tempIndex)
                                        .readOnly()
                                        .fileChannelEnable()
                                        .make();

                         HTreeMap<String, JumpSkipIndexEntries> positions = db.hashMap("index",
                                                                                       Serializer.STRING,
                                                                                       INDEX_SERIALIZER).open()) {

                        logger.warn("Opened temp index file");
                        Iterator<Map.Entry<String, JumpSkipIndexEntries>> entryIterator = positions.entrySet()
                                                                                                   .iterator();
                        if (entryIterator.hasNext()) {
                            logger.warn("Got first entry");
                            Map.Entry<String, JumpSkipIndexEntries> entry = entryIterator.next();

                            LastEventPositionInfo globalIndexEntry = globalIndex.get(entry.getKey());
                            logger.warn("Got entry from global index: {}", globalIndexEntry);
                            valid = globalIndexEntry != null && globalIndexEntry.lastSequence() >= entry.getValue()
                                                                                                        .firstSequenceNumber();
                        }
                    } catch (Exception ex) {
                        logger.warn("Failed to check {}", tempIndex, ex);
                    }

                    if (valid) {
                        Files.move(tempIndex.toPath(), storageProperties.newIndex(context, segment).toPath());
                    }
                }
            }
        } catch (Exception ex) {
            logger.warn("Failed to validate index for segment: {}", segment, ex);
        }
        return valid;
    }

    /**
     * Removes a segment from the index.
     *
     * @param segment the segment number
     */
    @Override
    public boolean remove(long segment) {
        if (activeIndexes.remove(segment) == null) {
            indexes.remove(segment);
            Index index = openIndexes.remove(segment);
            if (index != null) {
                index.close();
            }
        }
        return FileUtils.delete(storageProperties.newIndex(context, segment));
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

        long previousToken = -1;
        long minTokenInPreviousSegment = Long.MAX_VALUE;
        for (Long segment : activeIndexes.descendingKeySet()) {
            if (minTokenInPreviousSegment < minToken) {
                return results;
            }
            logger.debug("{}: lookupAggregate {} in segment {} map size {}",
                         context,
                         aggregateId,
                         segment,
                         activeIndexes.getOrDefault(segment, Collections
                                 .emptyMap()).size());

            JumpSkipIndexEntries entries = activeIndexes.getOrDefault(segment, Collections.emptyMap()).get(aggregateId);
            logger.debug("{}: lookupAggregate {} in segment {} found {}", context, aggregateId, segment, entries);
            if (entries != null) {
                IndexEntries entriesInRange = entries.range(firstSequenceNumber,
                                                            lastSequenceNumber,
                                                            EventType.SNAPSHOT.equals(eventType));
                if (!entriesInRange.isEmpty()) {
                    results.put(segment, entriesInRange);
                    maxResults -= entriesInRange.size();
                    if (allEntriesFound(firstSequenceNumber, maxResults, entries)) {
                        return results;
                    }
                }
                previousToken = entries.previousToken();
            }
            minTokenInPreviousSegment = segment;
        }

        if (previousToken < 0) {
            LastEventPositionInfo lastEntry = globalIndex.get(aggregateId);
            if (lastEntry != null) {
                previousToken = lastEntry.token();
            }
        }

        try {
            while (previousToken >= minToken && previousToken >= indexes.first()) {
                long segment = segmentForToken(previousToken);
                JumpSkipIndexEntries entries = getIndex(segment).get(aggregateId);
                IndexEntries range = entries.range(firstSequenceNumber,
                                                   lastSequenceNumber,
                                                   EventType.SNAPSHOT.equals(eventType));
                if (!range.isEmpty()) {
                    results.put(segment, range);
                    maxResults -= range.size();
                    if (allEntriesFound(firstSequenceNumber, maxResults, entries)) {
                        return results;
                    }
                }
                previousToken = entries.previousToken();
            }
        } catch (Exception exception) {
            // Index no longer in this tier
            logger.warn("{}: error reading events for {}, previous token {}, indexes {}", context,
                        aggregateId, previousToken, indexes);
        }

        return results;
    }

    private boolean allEntriesFound(long firstSequenceNumber, long maxResults, IndexEntries entries) {
        return firstSequenceNumber >= entries.firstSequenceNumber() || maxResults <= 0;
    }


    /**
     * Closes open files. Removes all index files and the global index if delete is set.
     *
     * @param delete flag to indicate that all indexes should be deleted
     */
    @Override
    public void cleanup(boolean delete) {
        activeIndexes.clear();
        openIndexes.forEach((segment, index) -> index.close());
        openIndexes.clear();
        globalIndex.close();

        if (delete) {
            for (Long index : indexes) {
                FileUtils.delete(storageProperties.newIndex(context, index));
            }
            globalIndex.delete();
        }

        if (cleanupTask != null && !cleanupTask.isDone()) {
            cleanupTask.cancel(true);
        }
    }

    @Override
    public Stream<String> getBackupFilenames(long lastSegmentBackedUp) {
        return Stream.concat(indexes.stream()
                                    .filter(s -> s > lastSegmentBackedUp)
                                    .map(s -> storageProperties.newIndex(context, s).getAbsolutePath()),
                             globalIndex.files());
    }

    private class Index implements Closeable {

        private final long segment;
        private final Object initLock = new Object();
        private volatile boolean initialized;
        private Map<String, JumpSkipIndexEntries> positions;
        private DB db;


        private Index(long segment) {
            this.segment = segment;
        }

        @Override
        public void close() {
            try {
                logger.debug("{}: close {}", segment, storageProperties.newIndex(context, segment));
                if (db != null && !db.isClosed()) {
                    indexCloseMeter.mark();
                    db.close();
                }
            } catch (Exception ex) {
                logger.warn("{}: closing {} failed", context, segment, ex);
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

                indexOpenMeter.mark();
                logger.debug("{}: open {}", segment, storageProperties.newIndex(context, segment));
                File indexFile = storageProperties.newIndex(context, segment);
                DBMaker.Maker maker = DBMaker.fileDB(indexFile).readOnly();
                if (storageProperties.isUseMmapIndex()) {
                    maker.fileMmapEnable();
                    if (storageProperties.isForceCleanMmapIndex()) {
                        maker.cleanerHackEnable();
                    }
                } else {
                    maker.fileChannelEnable();
                }
                this.db = maker.make();
                this.positions = db.hashMap("index", Serializer.STRING, INDEX_SERIALIZER).open();
                initialized = true;
            }
            return this;
        }

        public JumpSkipIndexEntries get(String key) {
            return positions.get(key);
        }
    }
}
