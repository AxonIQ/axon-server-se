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

/**
 * Interface describing a log entry store.
 *
 * @author Marc Gathier
 * @since 4.1
 */
public interface LogEntryStore {

    /**
     * Appends the given entries to the log entry store
     *
     * @param entries the entries to append
     * @throws IOException when there is a problem storing the entries
     */
    void appendEntry(List<Entry> entries) throws IOException;

    /**
     * Checks if the current log entry store contains an item with given {@code logIndex} and {@code term}.
     * @param logIndex the index of the log entry
     * @param logTerm  the expected term of the log entry
     * @return true if the log index with this index exists and has the given term
     */
    boolean contains(long logIndex, long logTerm);

    /**
     * Retrieves an entry from the log entry store. Returns {@code null} when the entry is not found.
     * @param index the index of the entry
     * @return the entry, or null when not found
     */
    Entry getEntry(long index);

    /**
     * Creates a new entry with a serialized object in the log entry store. Completes when the entry and all
     * previous entries are written.
     *
     * @param currentTerm the term to register with the new entry
     * @param entryType the type of entry
     * @param entryData the serialized data of the entry
     * @return completable future containing the created entry
     */
    CompletableFuture<Entry> createEntry(long currentTerm, String entryType, byte[] entryData);

    /**
     * Creates a new entry with RAFT cluster configuration in the log entry store. Completes when the entry and all
     * previous entries are written.
     *
     * @param currentTerm the term to register with the new entry
     * @param config the new raft configuration
     * @return completable future containing the created entry
     */
    CompletableFuture<Entry> createEntry(long currentTerm, Config config);

    /**
     * Creates a new marker entry that indicates that this node has become RAFT leader in the log entry store.
     *
     * Completes when the entry and all previous entries are written.
     *
     * @param currentTerm the term to register with the new entry
     * @param config the new raft configuration
     * @return completable future containing the created entry
     */
    CompletableFuture<Entry> createEntry(long currentTerm, LeaderElected config);

    long getTerm(long index);

    /**
     * Returns the term and index of the last entry in the log entry store.
     *
     * @return the term and index of the last entry in the log entry store
     */
    TermIndex lastLog();

    /**
     * Returns the term and index of the first entry in the log entry store.
     * @return the term and index of the first entry in the log entry store
     */
    TermIndex firstLog();

    /**
     * Create an iterator to iterate over the entries in the log entry store, starting with the given index.
     * @param index the first index to return in the iterator
     * @return iterator of log entries
     */
    EntryIterator createIterator(long index);

    /**
     * Removes all items from the log entry store, keeping {@code lastIndex} as the last index in the log entry store.
     * @param lastIndex the index of the last log entry to keep
     */
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

    /**
     * Returns the index of last entry in the log entry store.
     * @return index of last entry in the log entry store
     */
    default long lastLogIndex() {
        return lastLog().getIndex();
    }

    /**
     * Returns the index of first entry in the log entry store.
     * @return index of first entry in the log entry store
     */
    default long firstLogIndex() {
        return firstLog().getIndex();
    }

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

    /**
     * Deletes the complete log entry store.
     */
    void delete();

    /**
     * Gracefully closes all thread executors that are active
     * @param deleteData cleans up and removes store data
     */
    void close(boolean deleteData);
}
