package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.grpc.cluster.Entry;

/**
 * Consumes log entries and applies them to the state machine. Implementations should be idempotent and transactional.
 *
 * @author Marc Gathier
 * @since 4.1
 */
public interface LogEntryConsumer {

    /**
     * @return The name of the entry type in the log file consumed by this consumer
     */
    String entryType();

    /**
     * Processes a log entry.
     *
     * @param groupId the replication group
     * @param entry   the entry to apply
     * @throws Exception when processing of the entry failed
     */
    void consumeLogEntry(String groupId, Entry entry) throws Exception;
}
