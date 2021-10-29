package io.axoniq.axonserver.filestorage;

import io.axoniq.axonserver.filestorage.impl.FileStoreException;
import org.springframework.data.util.CloseableIterator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Generic append only file store.
 *
 * @author Marc Gathier
 * @since 4.6.0
 */
public interface FileStore {

    /**
     * Appends an entry to the store.
     * @param entry the entry to add
     * @return mono with index of the added entry
     */
    Mono<Long> append(FileStoreEntry entry);

    /**
     * Appends all entry published in the flux to the store. Entries are stored in a single transaction.
     * @param entriesPublisher the publisher for entries to add
     * @return mono with index of the first added entry
     */
    Mono<Long> append(Flux<FileStoreEntry> entriesPublisher);

    /**
     * Reads a single entry from the store.
     * @param index the index of the entry to read
     * @return the entry
     * @throws FileStoreException when the file store is empty
     */
    Mono<FileStoreEntry> read(long index);

    /**
     * Streams all entries in the store from the given {@code fromIndex}.
     * @param fromIndex index of the first entry in the stream
     * @return a stream of entries
     */
    Flux<FileStoreEntry> stream(long fromIndex);

    /**
     * Streams all entries in the store from the given {@code fromIndex} until {@code toIndex}.
     * If the value of {@code toIndex} is higher than the last index of the store, it returns the entries until the end
     * of the store.
     * @param fromIndex index of the first entry in the stream
     * @param toIndex index of the last entry in the stream to return (inclusive)
     * @return a stream of entries
     */
    Flux<FileStoreEntry> stream(long fromIndex, long toIndex);

    /**
     * Creates an iterator of entries from {@code fromIndex}
     * @param fromIndex index of the first entry in the iterator
     * @return iterator of entries
     */
    CloseableIterator<FileStoreEntry> iterator(int fromIndex);

    /**
     * Deletes the file store.
     */
    void delete();

    /**
     * Open the file store. Initialize a file store if it does not exist.
     * @param validate perform validations on open
     */
    void open(boolean validate);

    /**
     * Returns the last entry in the file store. Returns {@code null} when file store is empty.
     * @return entry or null
     */
    FileStoreEntry lastEntry();

    /**
     * Checks if the file store is empty.
     * @return {@code true} if file store is empty
     */
    boolean isEmpty();
}
