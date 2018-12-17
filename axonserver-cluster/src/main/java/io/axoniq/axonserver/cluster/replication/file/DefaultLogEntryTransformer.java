package io.axoniq.axonserver.cluster.replication.file;

/**
 * Author: marc
 */
public class DefaultLogEntryTransformer implements LogEntryTransformer {

    @Override
    public byte[] readLogEntry(byte[] bytes) {
        return bytes;
    }

    @Override
    public byte[] transform(byte[] bytes) {
        return bytes;
    }
}
