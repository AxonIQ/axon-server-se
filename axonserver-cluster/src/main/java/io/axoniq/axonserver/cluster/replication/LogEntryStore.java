package io.axoniq.axonserver.cluster.replication;

import io.axoniq.axonserver.cluster.TermIndex;
import io.axoniq.axonserver.grpc.cluster.Entry;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface LogEntryStore {

    void appendEntry(List<Entry> entries) throws IOException;

    boolean contains(long logIndex, long logTerm);

    Entry getEntry(long index);

    CompletableFuture<Entry> createEntry(long currentTerm, String entryType, byte[] entryData);

    TermIndex lastLog();

    EntryIterator createIterator(long index);

    void clear(long logIndex, long logTerm);

    long lastLogIndex();
}
