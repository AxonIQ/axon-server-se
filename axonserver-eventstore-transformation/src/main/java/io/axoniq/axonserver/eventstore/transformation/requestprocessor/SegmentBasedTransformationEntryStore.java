package io.axoniq.axonserver.eventstore.transformation.requestprocessor;

import io.axoniq.axonserver.filestorage.AppendOnlyFileStore;
import io.axoniq.axonserver.filestorage.FileStoreEntry;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class SegmentBasedTransformationEntryStore implements TransformationEntryStore {

    private final AppendOnlyFileStore appendOnlyFileStore;

    public SegmentBasedTransformationEntryStore(AppendOnlyFileStore appendOnlyFileStore) {
        this.appendOnlyFileStore = appendOnlyFileStore;
    }

    @Override
    public Mono<Long> store(TransformationEntry entry) {
        long sequence = entry.sequence();
        return resetIfNeeded(sequence).then(appendOnlyFileStore.append(toFileStoreEntry(entry)))
                                      .thenReturn(sequence);
    }

    @Override
    public Flux<TransformationEntry> read() {
        // TODO: 12/28/21
        return Flux.empty();
    }

    @Override
    public Mono<Void> delete() {
        return appendOnlyFileStore.delete();
    }

    private FileStoreEntry toFileStoreEntry(TransformationEntry entry) {
        return new FileStoreEntry() {
            @Override
            public byte[] bytes() {
                return entry.payload();
            }

            @Override
            public byte version() {
                return entry.version();
            }
        };
    }

    private Mono<Void> resetIfNeeded(long sequence) {
        return neededToResetTo(sequence).flatMap(this::reset);
    }

    /**
     * //TODO Returns the sequence to reset to. Returns an Empty mono if reset is not needed.
     *
     * @param sequence
     * @return
     */
    private Mono<Long> neededToResetTo(long sequence) {
        return appendOnlyFileStore.lastSequence()
                                  .filter(lastSequence -> lastSequence + 1 != sequence)
                                  .map(lastSequence -> sequence - 1);
    }

    private Mono<Void> reset(long sequence) {
        return appendOnlyFileStore.reset(sequence);
    }
}




