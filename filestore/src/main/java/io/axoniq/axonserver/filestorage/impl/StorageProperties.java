package io.axoniq.axonserver.filestorage.impl;

import java.io.File;

/**
 * @author Marc Gathier
 * @since 4.6.0
 */
public class StorageProperties {

    private static final String PATH_FORMAT = "%s/%020d%s";

    private String suffix = ".data";
    private long syncInterval = 1000;
    private long forceInterval = 1000;
    private long primaryCleanupDelay = 0;
    private long segmentSize = 1024 * 1024;
    private int flags = 0;
    private boolean forceClean = true;
    private File storage;
    private int validationSegments = 0;
    private long firstIndex = 0;

    public String getSuffix() {
        return suffix;
    }

    public void setSuffix(String suffix) {
        this.suffix = suffix;
    }

    public long getSyncInterval() {
        return syncInterval;
    }

    public void setSyncInterval(long syncInterval) {
        this.syncInterval = syncInterval;
    }

    public long getForceInterval() {
        return forceInterval;
    }

    public void setForceInterval(long forceInterval) {
        this.forceInterval = forceInterval;
    }

    public File dataFile(long segment) {
        return new File(String.format(PATH_FORMAT, storage.getAbsolutePath(), segment, suffix));
    }

    public long getPrimaryCleanupDelay() {
        return primaryCleanupDelay;
    }

    public void setPrimaryCleanupDelay(long primaryCleanupDelay) {
        this.primaryCleanupDelay = primaryCleanupDelay;
    }

    public long getSegmentSize() {
        return segmentSize;
    }

    public void setSegmentSize(long segmentSize) {
        this.segmentSize = segmentSize;
    }

    public int getFlags() {
        return flags;
    }

    public void setFlags(int flags) {
        this.flags = flags;
    }

    public boolean isForceClean() {
        return forceClean;
    }

    public void setForceClean(boolean forceClean) {
        this.forceClean = forceClean;
    }

    public File getStorage() {
        return storage;
    }

    public void setStorage(File storage) {
        this.storage = storage;
    }

    public int getValidationSegments() {
        return validationSegments;
    }

    public void setValidationSegments(int validationSegments) {
        this.validationSegments = validationSegments;
    }

    public long getFirstIndex() {
        return firstIndex;
    }

    public void setFirstIndex(long firstIndex) {
        this.firstIndex = firstIndex;
    }
}
