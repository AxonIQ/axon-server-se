package io.axoniq.axonserver.filestorage.impl;

import io.axoniq.axonserver.filestorage.FileStore;
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
public class BaseFileStore implements FileStore {

    private final WritableSegment primary;

    public BaseFileStore(StorageProperties storageProperties, String name) {
        this.primary = new WritableSegment(name,
                                           storageProperties,
                                           new ReadOnlySegments(name, storageProperties));
    }

    @Override
    public Mono<Long> append(FileStoreEntry entry) {
        return Mono.create(sink -> {
            primary.write(entry).whenComplete((token, ex) -> {
                if (ex != null) {
                    sink.error(ex);
                } else {
                    sink.success(token);
                }
            });
        });
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
    public Mono<FileStoreEntry> read(long index) {
        return Mono.create(sink -> {
            try (CloseableIterator<FileStoreEntry> it = primary.getEntryIterator(index)) {
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
    public Flux<FileStoreEntry> stream(long fromIndex) {
        CloseableIterator<FileStoreEntry> entryIterator = primary.getEntryIterator(fromIndex);
        return Flux.fromIterable(() -> entryIterator).doOnTerminate(entryIterator::close);
    }

    @Override
    public Flux<FileStoreEntry> stream(long fromIndex, long toIndex) {
        CloseableIterator<FileStoreEntry> entryIterator = primary.getEntryIterator(fromIndex, toIndex);
        return Flux.fromIterable(() -> entryIterator).doOnTerminate(entryIterator::close);
    }


    public void close() {
        primary.close(false);
    }

    public void open(boolean validate) {
        primary.init(validate);
    }

    public void delete() {
        primary.close(true);
    }

    public CloseableIterator<FileStoreEntry> iterator(int fromIndex) {
        return primary.getEntryIterator(fromIndex);
    }
}
