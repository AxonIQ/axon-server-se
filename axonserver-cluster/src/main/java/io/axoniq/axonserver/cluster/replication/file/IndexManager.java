package io.axoniq.axonserver.cluster.replication.file;

import io.axoniq.axonserver.cluster.exception.ErrorCode;
import io.axoniq.axonserver.cluster.exception.LogException;
import io.axoniq.axonserver.cluster.util.AxonThreadFactory;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
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
    private static final ScheduledExecutorService scheduledExecutorService =
            Executors.newScheduledThreadPool(1, new AxonThreadFactory("replication-index-"));
    private final StorageProperties storageProperties;
    private final ConcurrentSkipListMap<Long, Index> indexMap = new ConcurrentSkipListMap<>();
    private final String context;

    public IndexManager(StorageProperties storageProperties,
                        String context) {
        this.storageProperties = storageProperties;
        this.context = context;
        scheduledExecutorService.scheduleAtFixedRate(this::indexCleanup, 10, 10, TimeUnit.SECONDS);
    }


    public boolean validIndex(long segment) {
        try {
            return fileExists(segment) && getIndex(segment) != null;
        } catch (Exception ex) {
            logger.warn("{}: Failed to validate index for segment: {}", context, segment, ex);
        }
        return false;
    }

    private boolean fileExists(long segment) {
        return storageProperties.indexFile(context, segment).exists() &&
                storageProperties.indexFile(context, segment).canRead();
    }

    public Index getIndex(long segment) {
        return indexMap.computeIfAbsent(segment, Index::new).ensureReady();
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
        if (storageProperties.isUseMmapIndex()) {
            maker.fileMmapEnable();
            if (storageProperties.isForceCleanMmapIndex()) {
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

        try {
            Files.move(tempFile.toPath(), storageProperties.indexFile(context, segment).toPath(),
                       StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new LogException(ErrorCode.INDEX_WRITE_ERROR,
                                   "Failed to rename index file + storageProperties.indexFile(context, segment)",
                                   e);
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

        private final long segment;
        private final Object initLock = new Object();
        private volatile boolean initialized;
        private Map<Long, Integer> entriesMap;
        private DB db;

        private Index(long segment) {
            this.segment = segment;
        }

        public Integer getPosition(long index) {
            return entriesMap.get(index);
        }

        public void close() {
            logger.debug("{}: close {}", segment, storageProperties.indexFile(context, segment));
            db.close();
        }

        public Index ensureReady() {
            if (initialized && !db.isClosed()) {
                return this;
            }

            synchronized (initLock) {
                if (initialized && !db.isClosed()) {
                    return this;
                }
                logger.debug("{}: open {}", segment, storageProperties.indexFile(context, segment));

                DBMaker.Maker maker = DBMaker.fileDB(storageProperties.indexFile(context, segment))
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
                this.entriesMap = db.hashMap(INDEX_MAP, Serializer.LONG, Serializer.INTEGER).createOrOpen();
                initialized = true;
            }
            return this;
        }
    }
}
