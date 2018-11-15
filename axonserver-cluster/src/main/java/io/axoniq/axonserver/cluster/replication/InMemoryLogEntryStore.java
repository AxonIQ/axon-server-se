package io.axoniq.axonserver.cluster.replication;

import io.axoniq.axonserver.grpc.cluster.Entry;

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Author: marc
 */
public class InMemoryLogEntryStore implements LogEntryStore {

    private final NavigableMap<Long, Entry> entryMap = new ConcurrentSkipListMap<>();
    private final AtomicLong lastApplied = new AtomicLong(0);
    private final AtomicLong commitIndex = new AtomicLong(0);
    private final AtomicBoolean applyRunning = new AtomicBoolean(false);

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
        if( applyRunning.compareAndSet(false, true)) {
            while( lastApplied.get() < commitIndex.get()) {
                Entry entry = entryMap.get(lastApplied.get());
                consumer.accept(entry);
                lastApplied.incrementAndGet();
            }
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
        return Optional.ofNullable(entryMap.lastEntry())
                       .map(lastEntry -> lastEntry.getValue().getTerm())
                       .orElse(0L);
    }

    @Override
    public long lastLogIndex() {
        return Optional.ofNullable(entryMap.lastEntry())
                       .map(Map.Entry::getKey)
                       .orElse(0L);
    }
}
