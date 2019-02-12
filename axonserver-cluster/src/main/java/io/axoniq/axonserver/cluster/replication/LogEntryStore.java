package io.axoniq.axonserver.cluster.replication;

import io.axoniq.axonserver.cluster.Registration;
import io.axoniq.axonserver.cluster.TermIndex;
import io.axoniq.axonserver.grpc.cluster.Config;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.cluster.LeaderElected;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.stream.Stream;

public interface LogEntryStore {

    void appendEntry(List<Entry> entries) throws IOException;

    boolean contains(long logIndex, long logTerm);

    Entry getEntry(long index);

    CompletableFuture<Entry> createEntry(long currentTerm, String entryType, byte[] entryData);

    CompletableFuture<Entry> createEntry(long currentTerm, Config config);

    CompletableFuture<Entry> createEntry(long currentTerm, LeaderElected config);

    TermIndex lastLog();

    EntryIterator createIterator(long index);

    default EntryIterator createIterator(){
        return createIterator(1);
    }

    default void clear(){
        clear(0);
    }

    void clear(long lastIndex);

    /**
     * Performs a log compaction for logs older than the specified time.
     * All log entries starting from the one preceding the last applied entry will not be deleted.
     *
     * @param time the time before which the logs could be deleted
     * @param timeUnit the time unit
     * @param lastAppliedIndexSupplier the supplier of the last applied log entry index
     */
    void clearOlderThan(long time, TimeUnit timeUnit, LongSupplier lastAppliedIndexSupplier);

    long lastLogIndex();

    Registration registerLogAppendListener(Consumer<Entry> listener);

    Registration registerLogRollbackListener(Consumer<Entry> listener);

    /**
     * Returns the stream of filenames that are available for backing up.
     *
     * @return the stream of the full name of files available for backing up
     */
    default Stream<String> getBackupFilenames(){
        return Stream.empty();
    }

}
