package io.axoniq.axonserver.cluster;

/**
 * Author: marc
 */
public interface ProcessorStore {

    void updateLastApplied(long lastApplied);

    void updateCommitIndex(long commitIndex);

    long commitIndex();

    long lastApplied();
}
