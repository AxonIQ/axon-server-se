package io.axoniq.axonserver.cluster.replication;

import io.axoniq.axonserver.cluster.TermIndex;
import io.axoniq.axonserver.grpc.cluster.Entry;

import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;

public class InMemoryEntryIterator implements EntryIterator {

    private final LogEntryStore logEntryStore;
    private final AtomicLong currentIndex = new AtomicLong();
    private volatile Entry lastEntry = null;
    private volatile Entry currentEntry = null;

    public InMemoryEntryIterator(LogEntryStore logEntryStore, long start) {
        this.logEntryStore = logEntryStore;
        this.currentIndex.set(start);
        lastEntry = logEntryStore.getEntry(start-1);
    }

    @Override
    public boolean hasNext() {
        return currentIndex.get() <= logEntryStore.lastLogIndex();
    }

    @Override
    public Entry next() {
        if( currentIndex.get() > logEntryStore.lastLogIndex()) throw new NoSuchElementException();
        lastEntry = currentEntry;
        currentEntry = logEntryStore.getEntry(currentIndex.getAndIncrement());
        return currentEntry;
    }

    @Override
    public TermIndex previous() {
        if( lastEntry == null) return null;
        return new TermIndex(lastEntry.getTerm(), lastEntry.getIndex());
    }

}
