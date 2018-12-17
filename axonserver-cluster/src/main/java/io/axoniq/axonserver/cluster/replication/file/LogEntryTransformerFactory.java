package io.axoniq.axonserver.cluster.replication.file;

/**
 * Author: marc
 */
public interface LogEntryTransformerFactory {

    LogEntryTransformer get(byte version, int flags, StorageProperties storageProperties);
}
