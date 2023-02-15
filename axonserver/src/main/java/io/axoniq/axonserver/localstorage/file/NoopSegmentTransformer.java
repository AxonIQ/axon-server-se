package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axonserver.grpc.event.EventWithToken;
import reactor.core.publisher.Mono;

class NoopSegmentTransformer implements SegmentTransformer{

    @Override
    public Mono<Void> initialize() {
        return Mono.empty();
    }

    @Override
    public Mono<Void> transformEvent(EventWithToken transformedEvent) {
        return Mono.empty();
    }

    @Override
    public Mono<Void> completeSegment() {
        return Mono.empty();
    }

    @Override
    public Mono<Void> rollback(Throwable e) {
        return Mono.empty();
    }

    @Override
    public Mono<Void> cancel() {
        return Mono.empty();
    }

    @Override
    public long segment() {
        return -1L;
    }
}
