package io.axoniq.axonserver.refactoring.store.api;

import reactor.core.publisher.Mono;

/**
 * @author Sara Pellegrini
 * @since 4.6
 */
public interface LocalSnapshotStorage extends SnapshotStorage {

    Mono<Long> lastToken();

    Mono<Long> firstToken();
}
