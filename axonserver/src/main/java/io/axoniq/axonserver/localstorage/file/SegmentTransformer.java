package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axonserver.grpc.event.EventWithToken;
import reactor.core.publisher.Mono;

interface SegmentTransformer {

    Mono<SegmentTransformer> initialize();

    Mono<TransformationProgress> transformEvent(EventWithToken transformedEvent);

    Mono<Void> completeSegment();

    Mono<Void> rollback(Throwable e);

    Mono<Void> cancel();
}
