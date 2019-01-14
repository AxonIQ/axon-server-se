package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.*;

/**
 * @author Marc Gathier
 */
public class IndexManager {
    private static final Logger logger = LoggerFactory.getLogger(IndexManager.class);
    private static final String AGGREGATE_MAP = "aggregateMap";
    private final StorageProperties storageProperties;
    private final ConcurrentNavigableMap<Long, PersistedBloomFilter> bloomFilterPerSegment = new ConcurrentSkipListMap<>();
    private final ConcurrentSkipListMap<Long, Index> indexMap = new ConcurrentSkipListMap<>();
    private final String context;
    private final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);

    public IndexManager(String context, StorageProperties storageProperties) {
        this.storageProperties = storageProperties;
        this.context = context;
    }


    public void createIndex(Long segment, Map<String, SortedSet<PositionInfo>> positionsPerAggregate, boolean force) {
        File tempFile = storageProperties.indexTemp(context, segment);
        if( tempFile.exists() && (! force || ! FileUtils.delete(tempFile))) {
            return;
        }

        DB db = DBMaker.fileDB(tempFile)
                       .fileMmapEnable()
                       .cleanerHackEnable()
//                       .allocateStartSize(fileStorageProperties.getIndexInitialSize())
//                       .allocateIncrement(fileStorageProperties.getIndexNextSize())
                       .make();
        try (HTreeMap<String, SortedSet<PositionInfo>> map = db.hashMap(AGGREGATE_MAP,
                                                                   Serializer.STRING,
                                                                   PositionInfoSerializer.get())
                                                          .createOrOpen() ) {
            map.putAll(positionsPerAggregate);
        }
        db.close();

        if( ! tempFile.renameTo(storageProperties.index(context, segment)) ) {
            throw new MessagingPlatformException(ErrorCode.INDEX_WRITE_ERROR, "Failed to rename index file:" + tempFile);
        }

        PersistedBloomFilter filter = new PersistedBloomFilter(storageProperties.bloomFilter(context, segment).getAbsolutePath(),
                                                               positionsPerAggregate.keySet().size(), storageProperties.getBloomIndexFpp());
        filter.create();
        filter.insertAll(positionsPerAggregate.keySet());
        filter.store();
    }

    public SortedSet<PositionInfo> getPositions(long segment, String aggregateId) {
        if(notInBloomIndex(segment,aggregateId)) {
            return Collections.emptySortedSet();
        }

        IllegalAccessError lastError = new IllegalAccessError();
        for(int retry = 0 ; retry < 3; retry++ ) {
            try (Index idx = getIndex(segment)) {
                return idx.getPositions(aggregateId);
            } catch (IllegalAccessError ex) {
                lastError = ex;
            }
        }
        throw lastError;
    }

    public Index getIndex(long segment) {
        Index index = indexMap.get(segment);
        if( index == null || index.db.isClosed()) {
            index = new Index(segment, false);
            indexMap.put(segment, index);
            indexCleanup();
        }
        return index;
    }

    private void indexCleanup() {
        while( indexMap.size() > storageProperties.getMaxIndexesInMemory()) {
            Map.Entry<Long, Index> entry = indexMap.pollFirstEntry();
            logger.debug("Closing index {}", entry.getKey());
            scheduledExecutorService.schedule(() -> entry.getValue().db.close(), 2, TimeUnit.SECONDS);
        }

        while( bloomFilterPerSegment.size() > storageProperties.getMaxBloomFiltersInMemory()) {
            Map.Entry<Long, PersistedBloomFilter> removed = bloomFilterPerSegment.pollFirstEntry();
            logger.debug("Removed bloomfilter for {} from memory", removed.getKey());
        }

    }

    private boolean notInBloomIndex(Long segment, String aggregateId) {
        PersistedBloomFilter persistedBloomFilter = bloomFilterPerSegment.computeIfAbsent(segment, i->loadBloomFilter(segment));
        return persistedBloomFilter != null && !persistedBloomFilter.mightContain(aggregateId);
    }

    private PersistedBloomFilter loadBloomFilter(Long segment) {
        PersistedBloomFilter filter = new PersistedBloomFilter(storageProperties.bloomFilter(context, segment).getAbsolutePath(), 0, 0.03f);
        if( ! filter.fileExists()) return null;
        filter.load();
        return filter;
    }

    public boolean validIndex(long segment) {
        try {
            return loadBloomFilter(segment) != null && getIndex(segment) != null;
        } catch (Exception ex) {
            logger.warn("Failed to validate index for segment: {}", segment, ex);
        }
        return false;
    }

    public void cleanup() {
        bloomFilterPerSegment.clear();
        indexMap.forEach((segment, index) -> index.close());
    }

    public void remove(Long s) {
        Index index = indexMap.remove(s);
        if( index != null) index.close();
        bloomFilterPerSegment.remove(s);
    }


    public class Index implements Closeable {

        private final Map<String, SortedSet<PositionInfo>> positions;
        private final DB db;
        private final boolean managed;

        private Index(long  segment, boolean managed) {
            this.managed = managed;
            DBMaker.Maker maker = DBMaker.fileDB(storageProperties.index(context, segment))
                                             .readOnly()
                                             .fileMmapEnable()
                                             .fileLockDisable();
            if (true) {
                    maker.cleanerHackEnable();
            }
            this.db = maker.make();
            this.positions = db.hashMap(AGGREGATE_MAP, Serializer.STRING, PositionInfoSerializer.get()).createOrOpen();
        }

        public SortedSet<PositionInfo> getPositions(String aggregateId) {
            SortedSet<PositionInfo> aggregatePositions = positions.get(aggregateId);
            return aggregatePositions == null ? Collections.emptySortedSet() : aggregatePositions;
        }

        public Set<String> getKeys() {
            return positions.keySet();
        }

        public void close() {
            if( !managed) db.close();
        }
    }

}
