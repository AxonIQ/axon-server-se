/*
 *  Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axonserver.config.SystemInfoProvider;
import org.springframework.util.Assert;
import org.springframework.util.unit.DataSize;

import java.io.File;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Marc Gathier
 */
public class StorageProperties implements Cloneable {

    public static final String TRANSFORMED_SUFFIX = ".transformed";
    private static final String PATH_FORMAT = "%s/%020d%s";
    private static final String TEMP_PATH_FORMAT = PATH_FORMAT + ".temp";
    private static final String PATH_WITH_VERSION_FORMAT = "%s/%020d_%05d%s";
    private static final String FILE_WITH_VERSION_FORMAT = "%020d_%05d%s";
    private static final String FILE_FORMAT = "%020d%s";
    private static final String TEMP_PATH_WITH_VERSION_FORMAT = PATH_WITH_VERSION_FORMAT + ".temp";
    private static final String TRANSFORMED_PATH_WITH_VERSION_FORMAT = PATH_WITH_VERSION_FORMAT + TRANSFORMED_SUFFIX;
    private static final String OLD_PATH_FORMAT = "%s/%014d%s";
    private static final int DEFAULT_READ_BUFFER_SIZE = 1024 * 32;
    public static final String PRIMARY_STORAGE_KEY = "primary";
    /**
     * File suffix for events files.
     */
    private String eventsSuffix = ".events";
    /**
     * File suffix for index files.
     */
    private String indexSuffix = ".index";
    /**
     * File suffix for new index files.
     */
    private String newIndexSuffix = ".nindex";
    /**
     * File suffix for the global index file.
     */
    private String globalIndexSuffix = ".xref";
    /**
     * File suffix for bloom files.
     */
    private String bloomIndexSuffix = ".bloom";

    /**
     * Size for new storage segments.
     */
    private int segmentSize = 1024 * 1024 * 256;

    /**
     * Locations for segment files. Will create subdirectory per context.
     */
    private String storage = "./data";
    /**
     * Locations for segment files. Will create subdirectory per context.
     */
    private Map<String, String> storages = new HashMap<>();

    /**
     * False-positive percentage allowed for bloom index. Decreasing the value increases the size of the bloom indexes.
     */
    private float bloomIndexFpp = 0.03f;
    /**
     * Interval to force syncing files to disk (ms)
     */
    private long forceInterval = 1000;
    /**
     * Number of segments to validate to on startup after unclean shutdown.
     */
    private int validationSegments = 10;
    /**
     * Number of recent segments that Axon Server keeps memory mapped
     */
    private int memoryMappedSegments = 5;
    /**
     * Delay to clear ByteBuffers from off-heap memory for writable segments
     */
    private int primaryCleanupDelay = 15;
    /**
     * Delay to clear ByteBuffers from off-heap memory for read-only segments
     */
    private int secondaryCleanupDelay = 15;
    /**
     * Maximum number of indexes to keep open in memory
     */
    private int maxIndexesInMemory = 50;
    /**
     * Maximum number of bloom filters to keep in memory
     */
    private int maxBloomFiltersInMemory = 100;
    /**
     * Interval (ms) to check if there are files that are complete and can be closed
     */
    private long syncInterval = 1000;

    /**
     * Use memory mapped files for index files
     */
    private boolean useMmapIndex = true;
    /**
     * When using memory mapped files for indexes, let mapdb forcefully close the memory mapped files on close
     */
    private Boolean forceCleanMmapIndex;

    /**
     * Forcefully clean memory mapped files
     */
    private Boolean forceClean;

    /**
     * Define how many events to prefetch from disk when streaming events to the client
     */
    private int eventsPerSegmentPrefetch = 10;

    /**
     * Size of the buffer when reading from non-memory mapped files. Defaults to 32kiB.
     */
    private int readBufferSize = DEFAULT_READ_BUFFER_SIZE;

    private boolean keepOldVersions = false;

    private final SystemInfoProvider systemInfoProvider;
    private int flags;
    /**
     * Time to keep events in primary tier before deleting them, if secondary tier is defined.
     */
    private Duration[] retentionTime = new Duration[]{
            Duration.ofDays(7)
    };
    private String indexFormat;
    private int segmentsForSequenceNumberCheck = 10;

    public StorageProperties(SystemInfoProvider systemInfoProvider) {
        this.systemInfoProvider = systemInfoProvider;
    }

    public StorageProperties(SystemInfoProvider systemInfoProvider, String eventsSuffix, String indexSuffix,
                             String bloomIndexSuffix, String newIndexSuffix, String globalIndexSuffix) {
        this(systemInfoProvider);
        this.eventsSuffix = eventsSuffix;
        this.indexSuffix = indexSuffix;
        this.bloomIndexSuffix = bloomIndexSuffix;
        this.globalIndexSuffix = globalIndexSuffix;
        this.newIndexSuffix = newIndexSuffix;
    }

    public String getEventsSuffix() {
        return eventsSuffix;
    }

    public void setEventsSuffix(String eventsSuffix) {
        this.eventsSuffix = eventsSuffix;
    }

    public String getIndexSuffix() {
        return indexSuffix;
    }

    public void setIndexSuffix(String indexSuffix) {
        this.indexSuffix = indexSuffix;
    }

    public String getNewIndexSuffix() {
        return newIndexSuffix;
    }

    public void setNewIndexSuffix(String newIndexSuffix) {
        this.newIndexSuffix = newIndexSuffix;
    }

    public String getBloomIndexSuffix() {
        return bloomIndexSuffix;
    }

    public void setBloomIndexSuffix(String bloomIndexSuffix) {
        this.bloomIndexSuffix = bloomIndexSuffix;
    }

    public int getSegmentSize() {
        return segmentSize;
    }

    public void setSegmentSize(DataSize segmentSize) {
        Assert.isTrue(segmentSize.toBytes() <= Integer.MAX_VALUE,
                      "Segment size must be less than " + Integer.MAX_VALUE);
        Assert.isTrue(segmentSize.toBytes() > 0, "Segment size must be greater than 0");

        this.segmentSize = (int) segmentSize.toBytes();
    }

    public void setStorage(String storage) {
        this.storage = storage;

        if (storages != null) {
            if (!storages.containsKey(PRIMARY_STORAGE_KEY)) {
                storages.put(PRIMARY_STORAGE_KEY, storage);
            }
        } else {
            this.storages = new HashMap<>();
            storages.put(PRIMARY_STORAGE_KEY, storage);
        }
    }

    public void setStorages(Map<String, String> storages) {
        if (this.storages != null && this.storages.containsKey(PRIMARY_STORAGE_KEY)) {
            this.storages.putAll(storages);
        } else {
            this.storages = storages;
        }
    }

    public int getEventsPerSegmentPrefetch() {
        return eventsPerSegmentPrefetch;
    }

    public void setEventsPerSegmentPrefetch(int eventsPerSegmentPrefetch) {
        this.eventsPerSegmentPrefetch = eventsPerSegmentPrefetch;
    }


    public float getBloomIndexFpp() {
        return bloomIndexFpp;
    }

    public void setBloomIndexFpp(float bloomIndexFpp) {
        this.bloomIndexFpp = bloomIndexFpp;
    }

    public File bloomFilter(String customStorage, long segment) {
        return new File(String.format(PATH_FORMAT, customStorage, segment, bloomIndexSuffix));
    }
    public File bloomFilter(String customStorage, FileVersion segment) {
        if( segment.segmentVersion() == 0) return bloomFilter(customStorage, segment.segment());
        return new File(String.format(PATH_WITH_VERSION_FORMAT, customStorage, segment.segment(), segment.segmentVersion(), bloomIndexSuffix));
    }

    public File index(String customStorage, long segment) {
        return new File(String.format(PATH_FORMAT, customStorage, segment, indexSuffix));
    }
    public File index(String customStorage, FileVersion segment) {
        if( segment.segmentVersion() == 0) return index(customStorage, segment.segment());
        return new File(String.format(PATH_WITH_VERSION_FORMAT, customStorage, segment.segment(), segment.segmentVersion(), indexSuffix));
    }

    public File indexTemp(String storagePath, long segment) {
        return new File(String.format(TEMP_PATH_FORMAT, storagePath, segment, indexSuffix));
    }

    public File indexTemp(String storagePath, FileVersion segment) {
        return new File(String.format(TEMP_PATH_WITH_VERSION_FORMAT, storagePath, segment.segment(), segment.segmentVersion(), indexSuffix));
    }

    public File transformedIndex(String storagePath, FileVersion segment) {
        if( segment.segmentVersion() == 0) return transformedIndex(storagePath, segment.segment());
        return new File(String.format(TRANSFORMED_PATH_WITH_VERSION_FORMAT, storagePath, segment.segment(), segment.segmentVersion(), indexSuffix));
    }

    public File newTransformedIndex(String storagePath, FileVersion segment) {
        if( segment.segmentVersion() == 0) return transformedIndex(storagePath, segment.segment());
        return new File(String.format(TRANSFORMED_PATH_WITH_VERSION_FORMAT, storagePath, segment.segment(), segment.segmentVersion(), newIndexSuffix));
    }

    public File transformedIndex(String storagePath, long segment) {
        return new File(String.format(TEMP_PATH_FORMAT, storagePath, segment, indexSuffix));
    }

    public File newIndex(String storagePath, long segment) {
        return new File(String.format(PATH_FORMAT, storagePath, segment, newIndexSuffix));
    }
    public File newIndex(String storagePath, FileVersion segment) {
        if( segment.segmentVersion() == 0) return newIndex(storagePath, segment.segment());

        return new File(String.format(PATH_WITH_VERSION_FORMAT, storagePath, segment.segment(), segment.segmentVersion(), newIndexSuffix));
    }

    public File newIndexTemp(String storagePath, FileVersion segment) {
        if( segment.segmentVersion() == 0) return newIndexTemp(storagePath, segment.segment());

        return new File(String.format(TRANSFORMED_PATH_WITH_VERSION_FORMAT, storagePath, segment.segment(), segment.segmentVersion(), newIndexSuffix));
    }

    public File newIndexTemp(String storagePath, long segment) {
        return new File(String.format(TEMP_PATH_FORMAT, storagePath, segment, newIndexSuffix));
    }

    public String getGlobalIndexSuffix() {
        return globalIndexSuffix;
    }

    public void setGlobalIndexSuffix(String globalIndexSuffix) {
        this.globalIndexSuffix = globalIndexSuffix;
    }

    public String dataFile(long segment) {
        return String.format(FILE_FORMAT, segment, eventsSuffix);
    }

    public String dataFile(FileVersion segment) {
        if (segment.segmentVersion() == 0) {
            return dataFile(segment.segment());
        }
        return String.format(FILE_WITH_VERSION_FORMAT, segment.segment(), segment.segmentVersion(), eventsSuffix);
    }

    public File dataFile(String storagePath, FileVersion fileVersion) {
        return new File(storagePath + File.separator + dataFile(fileVersion));
    }

    public File transformedDataFile(String storagePath, FileVersion segment) {
        if (segment.segmentVersion() == 0) {
            throw new RuntimeException("cannot transform to version 0");
        }
        return new File(String.format(TRANSFORMED_PATH_WITH_VERSION_FORMAT,
                                      storagePath,
                                      segment.segment(),
                                      segment.segmentVersion(),
                                      eventsSuffix));
    }

    public long getForceInterval() {
        return forceInterval;
    }

    public void setForceInterval(long forceInterval) {
        this.forceInterval = forceInterval;
    }

    public int getFlags() {
        return flags;
    }

    public String getPrimaryStorage(String context) {
        return String.format("%s/%s", storages.getOrDefault(PRIMARY_STORAGE_KEY, storage), context);
    }

    public String getStorage(String storageName) {
        String storagePath = storages.get(storageName);
        if (storagePath == null) {
            if (PRIMARY_STORAGE_KEY.equals(storageName)) {
                return storage;
            }
            throw new IllegalStateException("Storage " + storageName + " not defined on this node." +
                                                    "To define storage set property: axoniq.axonserver.event.storage."
                                                    + storageName);
        }
        return storagePath;
    }

    public Boolean getForceClean() {
        return forceClean;
    }

    public void setForceClean(Boolean forceClean) {
        this.forceClean = forceClean;
    }

    public String getStorage(String storageName, String context) {
        return String.format("%s/%s", getStorage(storageName), context);
    }

    public Map<String, String> getAvailableStorages() {
        return storages;
    }

    public int getValidationSegments() {
        return validationSegments;
    }

    public void setValidationSegments(int validationSegments) {
        this.validationSegments = validationSegments;
    }

    public int getMemoryMappedSegments() {
        return memoryMappedSegments;
    }

    public void setMemoryMappedSegments(int memoryMappedSegments) {
        this.memoryMappedSegments = memoryMappedSegments;
    }

    public int getPrimaryCleanupDelay() {
        return primaryCleanupDelay;
    }

    public void setPrimaryCleanupDelay(int primaryCleanupDelay) {
        this.primaryCleanupDelay = primaryCleanupDelay;
    }

    public int getSecondaryCleanupDelay() {
        return secondaryCleanupDelay;
    }

    public void setSecondaryCleanupDelay(int secondaryCleanupDelay) {
        this.secondaryCleanupDelay = secondaryCleanupDelay;
    }

    public int getMaxIndexesInMemory() {
        return maxIndexesInMemory;
    }

    public void setMaxIndexesInMemory(int maxIndexesInMemory) {
        this.maxIndexesInMemory = maxIndexesInMemory;
    }

    public int getMaxBloomFiltersInMemory() {
        return maxBloomFiltersInMemory;
    }

    public void setMaxBloomFiltersInMemory(int maxBloomFiltersInMemory) {
        this.maxBloomFiltersInMemory = maxBloomFiltersInMemory;
    }

    public long getSyncInterval() {
        return syncInterval;
    }

    public void setSyncInterval(long syncInterval) {
        this.syncInterval = syncInterval;
    }

    public int getReadBufferSize() {
        return readBufferSize;
    }

    public void setReadBufferSize(int readBufferSize) {
        this.readBufferSize = readBufferSize;
    }

    public File oldDataFile(String storagePath, long segment) {
        return new File(String.format(OLD_PATH_FORMAT, storagePath, segment, eventsSuffix));
    }

    public File oldIndex(String storagePath, long segment) {
        return new File(String.format(OLD_PATH_FORMAT, storagePath, segment, indexSuffix));
    }

    public File oldBloomFilter(String storagePath, long segment) {
        return new File(String.format(OLD_PATH_FORMAT, storagePath, segment, bloomIndexSuffix));
    }

    public void setUseMmapIndex(Boolean useMmapIndex) {
        this.useMmapIndex = useMmapIndex;
    }

    public void setForceCleanMmapIndex(Boolean forceCleanMmapIndex) {
        this.forceCleanMmapIndex = forceCleanMmapIndex;
    }

    public boolean isForceCleanMmapIndex() {
        return forceCleanMmapIndex != null ?
                forceCleanMmapIndex :
                systemInfoProvider.javaOnWindows();
    }

    public boolean isUseMmapIndex() {
        return useMmapIndex;
    }

    public boolean isCleanRequired() {
        return Boolean.TRUE.equals(forceClean) || systemInfoProvider.javaOnWindows();
    }

    public void setSegmentsForSequenceNumberCheck(int segmentsForSequenceNumberCheck) {
        this.segmentsForSequenceNumberCheck = segmentsForSequenceNumberCheck;
    }

    public void setFlags(int flags) {
        this.flags = flags;
    }

    private StorageProperties cloneProperties() {
        try {
            return (StorageProperties) this.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public StorageProperties withSegmentSize(int segmentSize) {
        StorageProperties clone = cloneProperties();
        clone.segmentSize = segmentSize;
        return clone;
    }

    public StorageProperties withMaxBloomFiltersInMemory(int maxBloomFiltersInMemory) {
        StorageProperties clone = cloneProperties();
        clone.maxBloomFiltersInMemory = maxBloomFiltersInMemory;
        return clone;
    }

    public void setRetentionTime(Duration[] retentionTime) {
        this.retentionTime = retentionTime;
    }

    public long getRetentionTime(int tier) {
        if (tier < 0 || tier >= retentionTime.length) {
            return System.currentTimeMillis();
        }
        return retentionTime[tier].toMillis();
    }

    public String getIndexFormat() {
        return indexFormat;
    }

    public void setIndexFormat(String indexFormat) {
        this.indexFormat = indexFormat;
    }

    public StorageProperties withIndexFormat(String indexFormat) {
        StorageProperties clone = cloneProperties();
        clone.indexFormat = indexFormat;
        return clone;
    }

    public StorageProperties withMaxIndexesInMemory(int maxIndexesInMemory) {
        StorageProperties clone = cloneProperties();
        clone.maxIndexesInMemory = maxIndexesInMemory;
        return clone;
    }

    public StorageProperties withRetentionTime(Duration[] retentionTime) {
        StorageProperties clone = cloneProperties();
        clone.retentionTime = retentionTime;
        return clone;
    }

    public boolean isKeepOldVersions() {
        return keepOldVersions;
    }

    public void setKeepOldVersions(boolean keepOldVersions) {
        this.keepOldVersions = keepOldVersions;
    }

    public StorageProperties withKeepOldVersions(boolean keepOldVersions) {
        StorageProperties clone = cloneProperties();
        clone.keepOldVersions = keepOldVersions;
        return clone;
    }

    public int segmentsForSequenceNumberCheck() {
        return segmentsForSequenceNumberCheck;
    }

    public StorageProperties withForceClean(boolean forceClean) {
        StorageProperties clone = cloneProperties();
        clone.forceClean = forceClean;
        return clone;
    }
}
