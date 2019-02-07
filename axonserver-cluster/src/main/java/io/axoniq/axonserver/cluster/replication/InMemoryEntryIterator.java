package io.axoniq.axonserver.cluster.replication;

import io.axoniq.axonserver.cluster.TermIndex;
import io.axoniq.axonserver.grpc.cluster.Entry;

import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;

public class InMemoryEntryIterator implements EntryIterator {

    private final LogEntryStore logEntryStore;
    private final AtomicLong nextIndex = new AtomicLong();
    private volatile TermIndex previous;
    private volatile TermIndex current;

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
        previous = current != null ? current : termIndexOf(entry.getIndex()-1);
        current = new TermIndex(entry);
        return entry;
    }

    @Override
    public TermIndex previous() {
       return previous;
    }

    private TermIndex termIndexOf(long index){
        if (index == 0) return new TermIndex();
        return new TermIndex(logEntryStore.getEntry(index));
    }

}
