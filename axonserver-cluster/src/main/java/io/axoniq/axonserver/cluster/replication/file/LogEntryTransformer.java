package io.axoniq.axonserver.cluster.replication.file;

/**
 * @author Marc Gathier
 *
 */
public interface LogEntryTransformer {

    byte[] readLogEntry(byte[] bytes);

    byte[] transform(byte[] bytes);
}
