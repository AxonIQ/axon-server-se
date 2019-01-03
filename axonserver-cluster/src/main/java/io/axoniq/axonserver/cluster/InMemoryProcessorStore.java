package io.axoniq.axonserver.cluster;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Author: marc
 */
public class InMemoryProcessorStore implements ProcessorStore {
    private final AtomicLong lastApplied = new AtomicLong();
    private final AtomicLong lastAppliedTerm = new AtomicLong();
    private final AtomicLong commitIndex = new AtomicLong();
    private final AtomicLong commitTerm = new AtomicLong();

    @Override
    public void updateLastApplied(long lastAppliedIndex, long lastAppliedTerm) {
        this.lastApplied.set(lastAppliedIndex);
        this.lastAppliedTerm.set(lastAppliedTerm);
    }

    @Override
    public void updateCommit(long commitIndex, long commitTerm) {
        this.commitIndex.set(commitIndex);
        this.commitTerm.set(commitTerm);
    }

    @Override
    public long commitIndex() {
        return commitIndex.get();
    }

    @Override
    public long lastAppliedIndex() {
        return lastApplied.get();
    }

    @Override
    public long commitTerm() {
        return commitTerm.get();
    }

    @Override
    public long lastAppliedTerm() {
        return lastAppliedTerm.get();
    }
}
