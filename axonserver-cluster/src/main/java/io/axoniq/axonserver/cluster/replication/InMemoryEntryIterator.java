package io.axoniq.axonserver.cluster.replication;

import io.axoniq.axonserver.cluster.TermIndex;
import io.axoniq.axonserver.grpc.cluster.Entry;

import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;

public class InMemoryEntryIterator implements EntryIterator {

    private final LogEntryStore logEntryStore;
    private final AtomicLong nextIndex = new AtomicLong();
    private volatile Entry previous;
    private volatile Entry current;

    public InMemoryEntryIterator(LogEntryStore logEntryStore, long start) {
        this.logEntryStore = logEntryStore;
        this.nextIndex.set(start);
    }

    @Override
    public boolean hasNext() {
        return nextIndex.get() <= logEntryStore.lastLogIndex();
    }

    @Override
    public Entry next() {
        if (!hasNext()) throw new NoSuchElementException();
        Entry entry = logEntryStore.getEntry(nextIndex.getAndIncrement());
        previous = current != null ? current : logEntryStore.getEntry(entry.getIndex()-1);
        current = entry;
        return entry;
    }

    @Override
    public TermIndex previous() {
        if (current != null && current.getIndex() == 1) {
            return new TermIndex();
        }
        return (previous == null) ? null : new TermIndex(previous);
    }

}
