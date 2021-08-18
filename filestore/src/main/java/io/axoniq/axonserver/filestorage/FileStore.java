package io.axoniq.axonserver.filestorage;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * @author Marc Gathier
 * @since 4.6.0
 */
public interface FileStore {

    Mono<Long> append(FileStoreEntry bytes);

    Mono<Long> append(Flux<FileStoreEntry> bytesList);

    Mono<FileStoreEntry> read(long index);

    Flux<FileStoreEntry> stream(long fromIndex);

    Flux<FileStoreEntry> stream(long fromIndex, long toIndex);
}
