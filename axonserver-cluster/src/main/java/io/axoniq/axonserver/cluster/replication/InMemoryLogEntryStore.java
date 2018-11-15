package io.axoniq.axonserver.cluster.replication;

import io.axoniq.axonserver.grpc.cluster.Entry;

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Author: marc
 */
public class InMemoryLogEntryStore implements LogEntryStore {

    private final NavigableMap<Long, Entry> entryMap = new ConcurrentSkipListMap<>();
    private final AtomicLong lastApplied = new AtomicLong(-1);
    private final AtomicLong commitIndex = new AtomicLong(-1);
    private final AtomicBoolean applyRunning = new AtomicBoolean(false);
    private static final long BATCH_SIZE = 100;

    @Override
    public void appendEntry(List<Entry> entries) {
        if (entries.isEmpty()) {
            return;
        }

        long firstIndex = entries.get(0).getIndex();
        entryMap.tailMap(firstIndex).clear();
        entries.forEach(entry -> entryMap.put(entry.getIndex(), entry));
    }

    @Override
    public boolean contains(long logIndex, long logTerm) {
        if (entryMap.containsKey(logIndex)) {
            return entryMap.get(logIndex).getTerm() == logTerm;
        }
        return false;
    }

    @Override
    public void applyEntries(Consumer<Entry> consumer) {
        if (applyRunning.compareAndSet(false, true)) {
            NavigableMap<Long, Entry> toApply;
            do {
                toApply = entryMap.subMap(lastApplied.get(),
                                          false,
                                          Math.min(commitIndex.get(),
                                                   lastApplied.get() + BATCH_SIZE),
                                          true);
                Map.Entry<Long, Entry> entry;
                while ((entry = toApply.pollFirstEntry()) != null) {
                    consumer.accept(entry.getValue());
                    lastApplied.incrementAndGet();
                }
            } while (!toApply.isEmpty());
            applyRunning.set(false);
        }
    }

    @Override
    public void markCommitted(long committedIndex) {
        commitIndex.set(committedIndex);
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
    public Entry getEntry(long index) {
        return entryMap.get(index);
    }

    @Override
    public long lastLogTerm() {
        return entryMap.lastEntry().getValue().getTerm();
    }

    @Override
    public long lastLogIndex() {
        return entryMap.lastKey();
    }
}
