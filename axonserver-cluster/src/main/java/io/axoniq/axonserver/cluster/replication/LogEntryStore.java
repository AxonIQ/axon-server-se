package io.axoniq.axonserver.cluster.replication;

import io.axoniq.axonserver.cluster.TermIndex;
import io.axoniq.axonserver.grpc.cluster.Entry;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public interface LogEntryStore {

    void appendEntry(List<Entry> entries) throws IOException;

    boolean contains(long logIndex, long logTerm);

    void applyEntries(Consumer<Entry> consumer);

    void markCommitted(long committedIndex);

    long commitIndex();

    long lastAppliedIndex();

    Entry getEntry(long index);

    long lastLogTerm();

    long lastLogIndex();

    CompletableFuture<Entry> createEntry(long currentTerm, String entryType, byte[] entryData);

    TermIndex lastLog();

    Iterator<Entry> createIterator(long index);
}
