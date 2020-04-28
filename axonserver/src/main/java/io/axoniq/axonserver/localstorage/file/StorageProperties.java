/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axonserver.config.SystemInfoProvider;

import java.io.File;
import java.util.concurrent.TimeUnit;

/**
 * @author Marc Gathier
 */
public class StorageProperties {

    private static final String PATH_FORMAT = "%s/%s/%020d%s";
    private static final String TEMP_PATH_FORMAT = PATH_FORMAT + ".temp";
    private static final String OLD_PATH_FORMAT = "%s/%s/%014d%s";
    private static final int DEFAULT_READ_BUFFER_SIZE = 1024 * 32;
    /**
     * File suffix for events files.
     */
    private String eventsSuffix = ".events";
    /**
     * File suffix for index files.
     */
    private String indexSuffix = ".index";
    /**
     * File suffix for bloom files.
     */
    private String bloomIndexSuffix = ".bloom";

    /**
     * Size for new storage segments.
     */
    private long segmentSize = 1024 * 1024 * 256L;

    /**
     * Location for segment files. Will create subdirectory per context.
     */
    private String storage = "./data";
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
     * Number of segments to keep in primary location (only for multitier storage option)
     */
    private int numberOfSegments = 5;
    /**
     * Delay to clear ByfeBuffers from off-heap memory for writable segments
     */
    private int primaryCleanupDelay = 15;
    /**
     * Delay to clear ByfeBuffers from off-heap memory for read-only segments
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
     * Size of the buffer when reading from non-memory mapped files. Defaults to 32kiB.
     */
    private int readBufferSize = DEFAULT_READ_BUFFER_SIZE;
    private final SystemInfoProvider systemInfoProvider;
    private int flags;
    /**
     * Time to keep events in primary tier before deleting them, if secondary tier is defined.
     */
    private long[] retentionTime = {
            TimeUnit.DAYS.toMillis(7)
    };

    public StorageProperties(SystemInfoProvider systemInfoProvider) {
        this.systemInfoProvider = systemInfoProvider;
    }

    public StorageProperties(SystemInfoProvider systemInfoProvider, String eventsSuffix, String indexSuffix,
                             String bloomIndexSuffix) {
        this(systemInfoProvider);
        this.eventsSuffix = eventsSuffix;
        this.indexSuffix = indexSuffix;
        this.bloomIndexSuffix = bloomIndexSuffix;
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

    public String getBloomIndexSuffix() {
        return bloomIndexSuffix;
    }

    public void setBloomIndexSuffix(String bloomIndexSuffix) {
        this.bloomIndexSuffix = bloomIndexSuffix;
    }

    public long getSegmentSize() {
        return segmentSize;
    }

    public void setSegmentSize(long segmentSize) {
        this.segmentSize = segmentSize;
    }

    public String getStorage() {
        return storage;
    }

    public void setStorage(String storage) {
        this.storage = storage;
    }

    public float getBloomIndexFpp() {
        return bloomIndexFpp;
    }

    public void setBloomIndexFpp(float bloomIndexFpp) {
        this.bloomIndexFpp = bloomIndexFpp;
    }

    public File bloomFilter(String context, long segment) {
        return new File(String.format(PATH_FORMAT, storage, context, segment, bloomIndexSuffix));
    }

    public File index(String context, long segment) {
        return new File(String.format(PATH_FORMAT, storage, context, segment, indexSuffix));
    }

    public File indexTemp(String context, long segment) {
        return new File(String.format(TEMP_PATH_FORMAT, storage, context, segment, indexSuffix));
    }

    public File dataFile(String context, long segment) {
        return new File(String.format(PATH_FORMAT, storage, context, segment, eventsSuffix));
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

    public String getStorage(String context) {
        return String.format("%s/%s", storage, context);
    }

    public int getValidationSegments() {
        return validationSegments;
    }

    public void setValidationSegments(int validationSegments) {
        this.validationSegments = validationSegments;
    }

    public int getNumberOfSegments() {
        return numberOfSegments;
    }

    public void setNumberOfSegments(int numberOfSegments) {
        this.numberOfSegments = numberOfSegments;
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

    public File oldDataFile(String context, long segment) {
        return new File(String.format(OLD_PATH_FORMAT, storage, context, segment, eventsSuffix));
    }
    public File oldIndex(String context, long segment) {
        return new File(String.format(OLD_PATH_FORMAT, storage, context, segment, indexSuffix));
    }
    public File oldBloomFilter(String context, long segment) {
        return new File(String.format(OLD_PATH_FORMAT, storage, context, segment, bloomIndexSuffix));
    }

    public boolean isCleanerHackEnabled() {
        return systemInfoProvider.javaOnWindows() && ! systemInfoProvider.javaWithModules();
    }

    public boolean isUseMmapIndex() {
        return ! (systemInfoProvider.javaOnWindows() && systemInfoProvider.javaWithModules());
    }

    public boolean isCleanRequired() {
        return systemInfoProvider.javaOnWindows();
    }


    public void setFlags(int flags) {
        this.flags = flags;
    }

    public void setRetentionTime(long[] retentionTime) {
        this.retentionTime = retentionTime;
    }

    public long getRetentionTime(int tier) {
        if (tier < 0 || tier >= retentionTime.length) {
            return System.currentTimeMillis();
        }
        return retentionTime[tier];
    }
}
