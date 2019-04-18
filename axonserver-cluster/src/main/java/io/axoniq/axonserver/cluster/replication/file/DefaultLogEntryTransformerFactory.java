package io.axoniq.axonserver.cluster.replication.file;

/**
 * @author Marc Gathier
 */
public class DefaultLogEntryTransformerFactory implements LogEntryTransformerFactory {

    @Override
    public LogEntryTransformer get(byte version, int flags, StorageProperties storageProperties) {
        return new DefaultLogEntryTransformer();
    }
}
