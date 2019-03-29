package io.axoniq.axonserver.cluster.replication.file;

/**
 * @author Marc Gathier
 */
public interface LogEntryTransformerFactory {

    LogEntryTransformer get(byte version, int flags, StorageProperties storageProperties);
}
