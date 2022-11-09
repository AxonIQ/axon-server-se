package io.axoniq.axonserver.eventstore.transformation.requestprocessor;

import io.axoniq.axonserver.grpc.event.Event;
import reactor.core.publisher.Mono;

public interface Transformation {

    default Mono<TransformationState> deleteEvent(long tokenToDelete, long previousToken) {
        return Mono.error(new RuntimeException("Unsupported operation."));
    }

    default Mono<TransformationState> replaceEvent(long token, Event event, long sequence) {
        return Mono.error(new RuntimeException("Unsupported operation."));
    }

    default Mono<TransformationState> startCancelling() {
        return Mono.error(new RuntimeException("Unsupported operation."));
    }

    default Mono<TransformationState> markCancelled() {
        return Mono.error(new RuntimeException("Unsupported operation."));
    }

    default Mono<TransformationState> startApplying(long sequence, String applier) {
        return Mono.error(new RuntimeException("Unsupported operation."));
    }

    default Mono<TransformationState> markApplied() {
        return Mono.error(new RuntimeException("Unsupported operation."));
    }
}
