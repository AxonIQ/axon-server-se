package io.axoniq.axonserver.filestorage.impl;

import io.axoniq.axonserver.filestorage.AppendOnlyFileStore;
import io.axoniq.axonserver.filestorage.FileStoreEntry;
import org.springframework.data.util.CloseableIterator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * @author Marc Gathier
 * @since 4.6.0
 */
public class BaseAppendOnlyFileStore implements AppendOnlyFileStore {

    private final WritableSegment primary;
    private final Set<CloseableIterator<FileStoreEntry>> activeReaders = new CopyOnWriteArraySet<>();
    private final ResetLock resetLock = new ResetLock();

    public BaseAppendOnlyFileStore(StorageProperties storageProperties, String name) {
        this.primary = new WritableSegment(name,
                                           storageProperties,
                                           new ReadOnlySegments(name, storageProperties));
    }

    @Override
    public Mono<Long> append(FileStoreEntry entry) {
        return Mono.fromCompletionStage(() -> primary.write(entry))
                   .doFirst(resetLock::increaseActiveRequests)
                   .doOnTerminate(resetLock::decreaseActiveRequests);
    }

    @Override
    public Mono<Long> append(Flux<FileStoreEntry> entryFlux) {
        return entryFlux.collectList()
                        .flatMap(entries -> Mono.fromCompletionStage(primary.write(entries))
                                                .doFirst(resetLock::increaseActiveRequests)
                                                .doOnTerminate(resetLock::decreaseActiveRequests));
    }

    @Override
    public Mono<Void> reset(long sequence) {
        return Mono.create(sink -> {
            if (sequence > primary.getLastIndex()) {
                sink.error(new FileStoreException(FileStoreErrorCode.NOT_FOUND,
                                                  "Cannot reset to value higher than the last index"));
            }
            try {
                resetLock.startReset();
                activeReaders.forEach(CloseableIterator::close);
                activeReaders.clear();
                primary.reset(sequence);
                sink.success();
            } catch (Exception e) {
                sink.error(e.getCause());
            } finally {
                resetLock.stopReset();
            }
        });
    }

    @Override
    public Mono<FileStoreEntry> read(long sequence) {
        return Mono.<FileStoreEntry>create(sink -> {
            try (CloseableIterator<FileStoreEntry> it = primary.getEntryIterator(sequence)) {
                if (it.hasNext()) {
                    sink.success(it.next());
                } else {
                    sink.error(new FileStoreException(FileStoreErrorCode.NOT_FOUND, "Entry not found"));
                }
            } catch (Exception ex) {
                sink.error(ex);
            }
        }).doFirst(resetLock::increaseActiveRequests).doOnTerminate(resetLock::decreaseActiveRequests);
    }

    @Override
    public Flux<FileStoreEntry> stream(long fromSequence) {
        if (resetLock.resetting()) {
            throw new FileStoreException(FileStoreErrorCode.RESET_IN_PROGRESS, "Reset in progress");
        }
        CloseableIterator<FileStoreEntry> entryIterator = primary.getEntryIterator(fromSequence);
        activeReaders.add(entryIterator);
        return Flux.fromIterable(() -> entryIterator)
                   .doOnTerminate(() -> {
                       activeReaders.remove(entryIterator);
                       entryIterator.close();
                   });
    }

    @Override
    public Flux<FileStoreEntry> stream(long fromSequence, long toSequence) {
        if (resetLock.resetting()) {
            throw new FileStoreException(FileStoreErrorCode.RESET_IN_PROGRESS, "Reset in progress");
        }
        CloseableIterator<FileStoreEntry> entryIterator = primary.getEntryIterator(fromSequence, toSequence);
        activeReaders.add(entryIterator);
        return Flux.fromIterable(() -> entryIterator)
                   .doOnTerminate(() -> {
                       activeReaders.remove(entryIterator);
                       entryIterator.close();
                   });
    }


    @Override
    public Mono<Void> close() {
        return Mono.fromRunnable(() -> primary.close(false));
    }

    @Override
    public Mono<Void> open(boolean validate) {
        return Mono.<Void>fromRunnable(() -> primary.init(validate)).doOnSuccess(c -> resetLock.clear());
    }

    @Override
    public FileStoreEntry lastEntry() {
        return primary.lastEntry();
    }

    public Mono<Void> delete() {
        // TODO: 6/2/22 move to Mono
        // TODO: 6/2/22 verify this works
        return Mono.fromRunnable(() -> {
            activeReaders.forEach(CloseableIterator::close);
            primary.close(true);
        });
    }

    public CloseableIterator<FileStoreEntry> iterator(int fromIndex) {
        return primary.getEntryIterator(fromIndex);
    }

    @Override
    public boolean isEmpty() {
        return primary.isEmpty();
    }
}
