package io.axoniq.axonserver.cluster.replication.file;

import java.io.File;

/**
 * Author: marc
 */
public class StorageProperties {

    private static final String FILENAME_PATTERN = "%s" + File.separator + "%014d%s";

    private int syncInterval = 1000;
    private int forceInterval = 1000;
    private String logSuffix = ".log";
    private String indexSuffix = ".index";
    private int validationSegments = 10;
    private int flags = 0;
    private int primaryCleanupDelay = 1000;
    private int segmentSize = 1024*1024*16;
    private long numberOfSegments = 1000;
    private long secondaryCleanupDelay = 30000;
    private String logStorageFolder = "log";

    public int getSyncInterval() {
        return syncInterval;
    }

    public void setSyncInterval(int syncInterval) {
        this.syncInterval = syncInterval;
    }

    public int getForceInterval() {
        return forceInterval;
    }

    public void setForceInterval(int forceInterval) {
        this.forceInterval = forceInterval;
    }

    public String getLogSuffix() {
        return logSuffix;
    }

    public void setLogSuffix(String logSuffix) {
        this.logSuffix = logSuffix;
    }


    public int getValidationSegments() {
        return validationSegments;
    }

    public void setValidationSegments(int validationSegments) {
        this.validationSegments = validationSegments;
    }

    public int getFlags() {
        return flags;
    }

    public void setFlags(int flags) {
        this.flags = flags;
    }

    public int getPrimaryCleanupDelay() {
        return primaryCleanupDelay;
    }

    public void setPrimaryCleanupDelay(int primaryCleanupDelay) {
        this.primaryCleanupDelay = primaryCleanupDelay;
    }

    public int getSegmentSize() {
        return segmentSize;
    }

    public void setSegmentSize(int segmentSize) {
        this.segmentSize = segmentSize;
    }

    public long getNumberOfSegments() {
        return numberOfSegments;
    }

    public void setNumberOfSegments(long numberOfSegments) {
        this.numberOfSegments = numberOfSegments;
    }

    public long getSecondaryCleanupDelay() {
        return secondaryCleanupDelay;
    }

    public void setSecondaryCleanupDelay(long secondaryCleanupDelay) {
        this.secondaryCleanupDelay = secondaryCleanupDelay;
    }

    public String getLogStorageFolder() {
        return logStorageFolder;
    }

    public void setLogStorageFolder(String logStorageFolder) {
        this.logStorageFolder = logStorageFolder;
    }

    public String getIndexSuffix() {
        return indexSuffix;
    }

    public void setIndexSuffix(String indexSuffix) {
        this.indexSuffix = indexSuffix;
    }

    public String getStorage(GroupContext context) {
        return logStorageFolder + File.separator + context.getContext() + File.separator + context.getGroupId();
    }

    public File logFile(GroupContext context, Long segment) {
        return new File(String.format(FILENAME_PATTERN, getStorage(context), segment, logSuffix));
    }

    public File indexFile(GroupContext context, Long segment) {
        return new File(String.format(FILENAME_PATTERN, getStorage(context), segment, indexSuffix));
    }

    public File indexTempFile(GroupContext context, Long segment) {
        return new File(String.format(FILENAME_PATTERN, getStorage(context), segment, indexSuffix + ".temp"));
    }

}
