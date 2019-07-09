package io.axoniq.axonserver.cluster;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Version of processor store that only keeps the information in memory. Only for test purposes.
 *
 * @author Marc Gathier
 * @since 4.1
 */
public class InMemoryProcessorStore implements ProcessorStore {
    private final AtomicLong lastApplied = new AtomicLong();
    private final AtomicLong lastAppliedTerm = new AtomicLong();
    private final AtomicLong commitIndex = new AtomicLong();
    private final AtomicLong commitTerm = new AtomicLong();
    private final String groupId;

    public InMemoryProcessorStore() {
        this("default");
    }

    public InMemoryProcessorStore(String groupId) {
        this.groupId = groupId;
    }

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

    @Override
    public String groupId() {
        return groupId;
    }
}
