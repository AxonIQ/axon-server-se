package io.axoniq.axonserver.cluster;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Author: marc
 */
public class InMemoryProcessorStore implements ProcessorStore {
    private final AtomicLong lastApplied = new AtomicLong();
    private final AtomicLong commitIndex = new AtomicLong();

    @Override
    public void updateLastApplied(long lastApplied) {
        this.lastApplied.set(lastApplied);

    }

    @Override
    public void updateCommitIndex(long commitIndex) {
        this.commitIndex.set(commitIndex);
    }

    @Override
    public long commitIndex() {
        return commitIndex.get();
    }

    @Override
    public long lastApplied() {
        return lastApplied.get();
    }
}
