package io.axoniq.axonserver.eventstore.transformation.compact;

import reactor.core.publisher.Mono;

public interface EventStoreCompactionExecutor {

    Mono<Void> compact(Compaction compaction);

    interface Compaction {

        String context();
    }
}
