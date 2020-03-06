package io.axoniq.axonserver.cluster.replication.file;

import io.axoniq.axonserver.cluster.exception.ErrorCode;
import io.axoniq.axonserver.cluster.exception.LogException;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author Marc Gathier
 */
public class IndexManager {

    private static final Logger logger = LoggerFactory.getLogger(IndexManager.class);
    private static final String INDEX_MAP = "indexMap";
    private static final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
    private final StorageProperties storageProperties;
    private final ConcurrentSkipListMap<Long, Index> indexMap = new ConcurrentSkipListMap<>();
    private final String context;

    public IndexManager(StorageProperties storageProperties,
                        String context) {
        this.storageProperties = storageProperties;
        this.context = context;
    }


    public boolean validIndex(long segment) {
        try {
            return getIndex(segment) != null;
        } catch (Exception ex) {
            logger.warn("{}: Failed to validate index for segment: {}", context, segment, ex);
        }
        return false;
    }

    public Index getIndex(long segment) {
        Index index = indexMap.get(segment);
        if (index != null && !index.isClosed()) {
            return index;
        }

        synchronized (indexMap) {
            index = indexMap.get(segment);
            if (index == null || index.isClosed()) {
                logger.debug("{}: open index for {}", context, segment);
                index = new IndexManager.Index(segment, false);
                indexMap.put(segment, index);
                indexCleanup();
            }
        }
        return index;
    }

    private void indexCleanup() {
        while (indexMap.size() > storageProperties.getMaxIndexesInMemory()) {
            Map.Entry<Long, Index> entry = indexMap.pollFirstEntry();
            logger.debug("{}: Closing index {}", context, entry.getKey());
            scheduledExecutorService.schedule(() -> entry.getValue().close(), 2, TimeUnit.SECONDS);
        }
    }

    public void createIndex(Long segment, Map<Long, Integer> entriesMap, boolean force) {
        File tempFile = storageProperties.indexTempFile(context, segment);
        if( tempFile.exists() && (! force || ! FileUtils.delete(tempFile))) {
            return;
        }

        DBMaker.Maker maker = DBMaker.fileDB(tempFile);
        if( storageProperties.isUseMmapIndex()) {
            maker.fileMmapEnable();
            if( storageProperties.isCleanerHackEnabled()) {
                maker.cleanerHackEnable();
            }
        } else {
            maker.fileChannelEnable();
        }
        DB db = maker.make();
        try (HTreeMap<Long, Integer> map = db.hashMap(INDEX_MAP,
                                                      Serializer.LONG,
                                                      Serializer.INTEGER)
                                             .createOrOpen()) {
            map.putAll(entriesMap);
        }
        db.close();

        if( ! tempFile.renameTo(storageProperties.indexFile(context, segment)) ) {
            throw new LogException(ErrorCode.INDEX_WRITE_ERROR, context + ": Failed to rename index file:" + tempFile);
        }
    }

    public void cleanup() {
        indexMap.forEach((segment, index) -> index.close());
    }

    public boolean remove(long segment) {
        Index index = indexMap.remove(segment);
        if( index != null) {
            try {
                index.close();
            } catch( Exception ex) {
                // No action
            }
        }
        return true;
    }

    public class Index implements Closeable {

        private final Map<Long, Integer> entriesMap;
        private final DB db;
        private final boolean managed;

        private Index(long  segment, boolean managed) {
            this.managed = managed;
            DBMaker.Maker maker = DBMaker.fileDB(storageProperties.indexFile(context, segment))
                                         .readOnly()
                                         .fileLockDisable();
            if( storageProperties.isUseMmapIndex()) {
                maker.fileMmapEnable();
                if (storageProperties.isCleanerHackEnabled()) {
                    maker.cleanerHackEnable();
                }
            } else {
                maker.fileChannelEnable();
            }
            this.db = maker.make();
            this.entriesMap = db.hashMap(INDEX_MAP, Serializer.LONG, Serializer.INTEGER).createOrOpen();
        }

        public Integer getPosition(long index) {
            return entriesMap.get(index);
        }

        public void close() {
            if( !managed) db.close();
        }

        public boolean isClosed() {
            return db.isClosed();
        }
    }
}
