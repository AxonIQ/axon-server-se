package io.axoniq.axonserver.cluster.replication;

import io.axoniq.axonserver.grpc.cluster.Entry;

import java.io.IOException;
import java.util.List;
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

}
