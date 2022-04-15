package io.axoniq.axonserver.filestorage.impl;

import io.axoniq.axonserver.filestorage.AppendOnlyFileStore;
import io.axoniq.axonserver.filestorage.FileStoreEntry;
import org.springframework.data.util.CloseableIterator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.LinkedList;
import java.util.List;

/**
 * @author Marc Gathier
 * @since 4.6.0
 */
public class BaseAppendOnlyFileStore implements AppendOnlyFileStore {

    private final WritableSegment primary;

    public BaseAppendOnlyFileStore(StorageProperties storageProperties, String name) {
        this.primary = new WritableSegment(name,
                                           storageProperties,
                                           new ReadOnlySegments(name, storageProperties));
    }

    @Override
    public Mono<Long> append(FileStoreEntry entry) {
        return Mono.fromCompletionStage(() -> primary.write(entry));
    }

    @Override
    public Mono<Long> append(Flux<FileStoreEntry> entryFlux) {
        return Mono.create(sink -> {
            List<FileStoreEntry> entries = new LinkedList<>();
            entryFlux.subscribe(entries::add,
                                sink::error,
                                () -> primary.write(entries)
                                             .whenComplete((token, error) -> {
                                                 if (error != null) {
                                                     sink.error(error);
                                                 } else {
                                                     sink.success(token);
                                                 }
                                             }));
        });
    }

    @Override
    public Mono<FileStoreEntry> read(long sequence) {
        return Mono.create(sink -> {
            try (CloseableIterator<FileStoreEntry> it = primary.getEntryIterator(sequence)) {
                if (it.hasNext()) {
                    sink.success(it.next());
                } else {
                    sink.error(new FileStoreException(FileStoreErrorCode.NOT_FOUND, "Entry not found"));
                }
            } catch (Exception ex) {
                sink.error(ex);
            }
        });
    }

    @Override
    public Flux<FileStoreEntry> stream(long fromSequence) {
        CloseableIterator<FileStoreEntry> entryIterator = primary.getEntryIterator(fromSequence);
        return Flux.fromIterable(() -> entryIterator).doOnTerminate(entryIterator::close);
    }

    @Override
    public Flux<FileStoreEntry> stream(long fromSequence, long toSequence) {
        CloseableIterator<FileStoreEntry> entryIterator = primary.getEntryIterator(fromSequence, toSequence);
        return Flux.fromIterable(() -> entryIterator).doOnTerminate(entryIterator::close);
    }


    @Override
    public Mono<Void> close() {
        return Mono.fromRunnable(() -> primary.close(false));
    }

    @Override
    public Mono<Void> open(boolean validate) {
        return Mono.fromRunnable(() -> primary.init(validate));
    }

    @Override
    public FileStoreEntry lastEntry() {
        return primary.lastEntry();
    }

    public void delete() {
        primary.close(true);
    }

    public CloseableIterator<FileStoreEntry> iterator(int fromIndex) {
        return primary.getEntryIterator(fromIndex);
    }

    @Override
    public boolean isEmpty() {
        return primary.isEmpty();
    }
}
