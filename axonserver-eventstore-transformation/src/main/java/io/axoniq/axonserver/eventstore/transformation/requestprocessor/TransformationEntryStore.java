package io.axoniq.axonserver.eventstore.transformation.requestprocessor;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface TransformationEntryStore {

    Mono<Long> store(TransformationEntry entry);

    Flux<TransformationEntry> read();

    default Flux<TransformationEntry> readFrom(long sequence) {
        return read().skipUntil(entry -> entry.sequence() < sequence);
    }

    default Flux<TransformationEntry> read(long fromSequence, long toSequence) {
        return readFrom(fromSequence).takeUntil(entry -> entry.sequence() < toSequence);
    }

    default Flux<TransformationEntry> readTo(long sequence) {
        return read().takeUntil(entry -> entry.sequence() < sequence);
    }

    Mono<Void> delete();
}
