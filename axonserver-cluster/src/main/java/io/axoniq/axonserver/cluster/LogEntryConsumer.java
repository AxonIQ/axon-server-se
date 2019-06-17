package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.grpc.cluster.Entry;

/**
 * Consumes log entries and applies them to the state machine. Implementations should be idempotent.
 *
 * @author Marc Gathier
 */
public interface LogEntryConsumer {

    String entryType();

    void consumeLogEntry(String groupId, Entry entry) throws Exception;

    default int priority() {
        return 0;
    }
}
