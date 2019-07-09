package io.axoniq.axonserver.cluster;

/**
 * Persists configuration for the Raft processor for a single raft group.
 *
 * @author Marc Gathier
 * @since 4.1
 */
public interface ProcessorStore {

    void updateLastApplied(long lastApplied, long term);

    void updateCommit(long commitIndex, long term);

    long commitIndex();

    long lastAppliedIndex();

    long commitTerm();

    long lastAppliedTerm();

    /**
     * Returns the raft group id for the processor store.
     *
     * @return raft group id for the processor store
     */
    String groupId();
}
