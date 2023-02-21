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
public interface AppendOnlyFileStore {

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
     * Resets the file store to have {@code sequence} as the last entry.
     * @todo Any open streams will be forcefully closed.
     * @param sequence the last entry to keep in the file store
     * @return mono when the reset is completed
     */
    Mono<Void> reset(long sequence);

    /**
     * Reads a single entry from the store.
     * @param sequence the sequence of the entry to read
     * @return the entry
     * @throws FileStoreException when the file store is empty  //TODO do we really want to thrown an exception i.o. completing exceptionally the mono?
     */
    Mono<FileStoreEntry> read(long sequence);

    /**
     * Streams all entries in the store from the given {@code fromSequence}.
     * @param fromSequence sequence of the first entry in the stream
     * @return a stream of entries
     */
    Flux<FileStoreEntry> stream(long fromSequence);

    /**
     * Streams all entries in the store from the given {@code fromSequence} until {@code toSequence}. If the value of
     * {@code toSequence} is higher than the last sequence of the store, it returns the entries until the end of the
     * store.
     *
     * @param fromSequence sequence of the first entry in the stream
     * @param toSequence   sequence of the last entry in the stream to return (inclusive)
     * @return a stream of entries
     */
    Flux<FileStoreEntry> stream(long fromSequence, long toSequence);

    /**
     * Creates an iterator of entries from {@code fromIndex}
     * @param fromIndex index of the first entry in the iterator
     * @return iterator of entries
     */
    // TODO: 12/30/21 remove
    CloseableIterator<FileStoreEntry> iterator(int fromIndex);

    /**
     * Deletes the file store.
     */
    // TODO: 12/30/21 Mono<Void>?
    Mono<Void> delete();

    /**
     * Open the file store. Initialize a file store if it does not exist.
     * @param validate perform validations on open
     */
    // TODO: 12/30/21 validate?
    Mono<Void> open(boolean validate);

    Mono<Void> close();

    /**
     * Returns the last entry in the file store. Returns {@code null} when file store is empty.
     * @return entry or null
     */
    // TODO: 12/30/21 check usage
    FileStoreEntry lastEntry();

    /**
     * Checks if the file store is empty.
     * @return {@code true} if file store is empty
     */
    boolean isEmpty();
    
    default Mono<Long> lastSequence() {
        // TODO: 12/30/21  
        return Mono.empty();
    }
}
