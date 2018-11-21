package io.axoniq.axonserver.cluster.replication;

import io.axoniq.axonserver.cluster.TermIndex;
import io.axoniq.axonserver.grpc.cluster.Entry;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;

public class EntryIterator implements Iterator<Entry> {

    private final LogEntryStore logEntryStore;
    private final AtomicLong currentIndex = new AtomicLong();
    private volatile Entry lastEntry = null;
    private volatile Entry currentEntry = null;

    public EntryIterator(LogEntryStore logEntryStore, long start) {
        this.logEntryStore = logEntryStore;
        this.currentIndex.set(start);
    }

    @Override
    public boolean hasNext() {
        if( lastEntry == null) {
            lastEntry = logEntryStore.getEntry(currentIndex.get()-1);
        }
        return currentIndex.get() <= logEntryStore.lastLogIndex();
    }

    @Override
    public Entry next() {
        if( currentIndex.get() > logEntryStore.lastLogIndex()) throw new NoSuchElementException();
        lastEntry = currentEntry;
        currentEntry = logEntryStore.getEntry(currentIndex.getAndIncrement());
        return currentEntry;
    }

    public TermIndex previous() {
        if( lastEntry == null) return null;
        return new TermIndex(lastEntry.getTerm(), lastEntry.getIndex());
    }
}
