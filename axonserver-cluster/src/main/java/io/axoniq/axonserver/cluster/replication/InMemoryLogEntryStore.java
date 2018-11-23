package io.axoniq.axonserver.cluster.replication;

import com.google.protobuf.ByteString;
import io.axoniq.axonserver.cluster.TermIndex;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.cluster.SerializedObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

/**
 * Author: marc
 */
public class InMemoryLogEntryStore implements LogEntryStore {
    private final Logger logger = LoggerFactory.getLogger(InMemoryLogEntryStore.class);
    private final NavigableMap<Long, Entry> entryMap = new ConcurrentSkipListMap<>();
    private final AtomicLong lastApplied = new AtomicLong(0);
    private final AtomicLong commitIndex = new AtomicLong(0);
    private final AtomicLong lastIndex = new AtomicLong(0);
    private final AtomicBoolean applyRunning = new AtomicBoolean(false);
    private Thread commitListenerThread;

    @Override
    public void appendEntry(List<Entry> entries) throws IOException {
        if (entries.isEmpty()) {
            return;
        }

        long firstIndex = entries.get(0).getIndex();
        if( entryMap.containsKey(firstIndex)) {
            logger.warn("Clear from {}", firstIndex);
            entryMap.tailMap(firstIndex).clear();
        }
        entries.forEach(entry -> entryMap.put(entry.getIndex(), entry));
        lastIndex.set(entryMap.lastEntry().getKey());
    }

    @Override
    public boolean contains(long logIndex, long logTerm) {
        if (logIndex == 0) {
            return true;
        }
        if (entryMap.containsKey(logIndex)) {
            return entryMap.get(logIndex).getTerm() == logTerm;
        }
        return false;
    }

    @Override
    public int applyEntries(Consumer<Entry> consumer) {
        int count = 0;
        if( applyRunning.compareAndSet(false, true)) {
            while( lastApplied.get() < commitIndex.get()) {
                Entry entry = entryMap.get(lastApplied.get()+1);
                consumer.accept(entry);
                lastApplied.incrementAndGet();
                count++;
            }
            applyRunning.set(false);
        }
        return count;
    }

    @Override
    public void markCommitted(long committedIndex) {
        if( committedIndex > commitIndex.get()) {
            commitIndex.set(committedIndex);
            logger.trace( "Committed: {}", committedIndex);
            if( commitListenerThread != null) {
                LockSupport.unpark(commitListenerThread);
            }
        }
    }

    @Override
    public void registerCommitListener(Thread currentThread) {
        commitListenerThread = currentThread;
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

    @Override
    public TermIndex lastLog() {
        if( entryMap.isEmpty()) return new TermIndex(0,0);
        Map.Entry<Long, Entry> entry = entryMap.lastEntry();
        return new TermIndex(entry.getValue().getTerm(), entry.getKey());
    }

    @Override
    public EntryIterator createIterator(long index) {
        logger.debug("Create iterator: {}", index);
        if( ! entryMap.isEmpty() && index < entryMap.firstKey()) {
            throw new IllegalArgumentException("Index before start");
        }
        return new EntryIterator(this, index);
    }

    @Override
    public CompletableFuture<Entry> createEntry(long currentTerm, String entryType, byte[] entryData) {
        long index = lastIndex.incrementAndGet();
        Entry entry = Entry.newBuilder()
                           .setIndex(index)
                           .setTerm(currentTerm)
                           .setSerializedObject(SerializedObject.newBuilder()
                                                                .setData(ByteString.copyFrom(entryData))
                                                                .setType(entryType))
                           .build();
        entryMap.put(index, entry);
        return CompletableFuture.completedFuture(entry);
    }
}
