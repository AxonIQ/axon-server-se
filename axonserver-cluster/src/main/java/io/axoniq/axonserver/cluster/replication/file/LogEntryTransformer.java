package io.axoniq.axonserver.cluster.replication.file;

/**
 * Author: marc
 */
public interface LogEntryTransformer {

    byte[] readLogEntry(byte[] bytes);

    byte[] transform(byte[] bytes);
}
