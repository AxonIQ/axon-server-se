package io.axoniq.axonserver.cluster.replication.file;

/**
 * Author: marc
 */
public class DefaultLogEntryTransformerFactory implements LogEntryTransformerFactory {

    @Override
    public LogEntryTransformer get(byte version, int flags, StorageProperties storageProperties) {
        return new DefaultLogEntryTransformer();
    }
}
