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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Author: marc
 */
public class InMemoryLogEntryStore implements LogEntryStore {
    private final Logger logger = LoggerFactory.getLogger(InMemoryLogEntryStore.class);
    private final NavigableMap<Long, Entry> entryMap = new ConcurrentSkipListMap<>();
    private final AtomicLong lastIndex = new AtomicLong(0);
    private final String name;

    public InMemoryLogEntryStore(String name) {
        this.name = name;
    }

    @Override
    public void appendEntry(List<Entry> entries) throws IOException {
        if (entries.isEmpty()) {
            return;
        }

        long firstIndex = entries.get(0).getIndex();
        if( entryMap.containsKey(firstIndex)) {
            logger.warn("{}: Clear from {}", name, firstIndex);
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
    public void clear(long logIndex, long logTerm) {
        if (!contains(logIndex, logTerm)) {
            // If existing log entry has same index and term as snapshot's last included entry, retain log entries
            // following it
            entryMap.headMap(logIndex, true)
                    .clear();
        } else {
            // Otherwise, discard the log
            entryMap.clear();
        }
    }

    @Override
    public Entry getEntry(long index) {
        return entryMap.get(index);
    }

    @Override
    public TermIndex lastLog() {
        if( entryMap.isEmpty()) return new TermIndex(0,0);
        Map.Entry<Long, Entry> entry = entryMap.lastEntry();
        return new TermIndex(entry.getValue().getTerm(), entry.getKey());
    }

    @Override
    public long lastLogIndex() {
        return entryMap.isEmpty() ? 0 : entryMap.lastKey();
    }

    @Override
    public EntryIterator createIterator(long index) {
        logger.debug("{}: Create iterator: {}", name, index);
        if( ! entryMap.isEmpty() && index < entryMap.firstKey()) {
            throw new IllegalArgumentException("Index before start");
        }
        return new InMemoryEntryIterator(this, index);
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
