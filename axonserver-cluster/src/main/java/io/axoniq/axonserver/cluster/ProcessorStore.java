package io.axoniq.axonserver.cluster;

/**
 * @author Marc Gathier
 */
public interface ProcessorStore {

    void updateLastApplied(long lastApplied, long term);

    void updateCommit(long commitIndex, long term);

    long commitIndex();

    long lastAppliedIndex();

    long commitTerm();

    long lastAppliedTerm();

    default String groupId() {
        return "ProcessorStore";
    }
}
