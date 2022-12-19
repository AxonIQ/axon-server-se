package io.axoniq.axonserver.eventstore.transformation.requestprocessor;

import reactor.core.publisher.Mono;

/**
 * @author Sara Pellegrini
 * @since 2023.0
 */
public interface EventStoreStateStore {

    Mono<EventStoreState> state(String context);

    Mono<Void> save(EventStoreState state);

    enum State {
        IDLE,
        COMPACTING,
        TRANSFORMING
    }

    interface Visitor {

        Visitor setContext(String context);

        Visitor setState(State state);

        Visitor setOperationId(String operationId);
    }

    interface EventStoreState {

        void accept(Visitor visitor);

        default Mono<EventStoreState> transform(String transformationId) {
            return Mono.error(new WrongTransformationStateException("Unsupported operation"));
        }

        default Mono<EventStoreState> transformed() {
            return Mono.error(new WrongTransformationStateException("Unsupported operation"));
        }

        default Mono<EventStoreState> compact(String compactionId) {
            return Mono.error(new WrongTransformationStateException("Unsupported operation"));
        }

        default Mono<EventStoreState> compacted() {
            return Mono.error(new WrongTransformationStateException("Unsupported operation"));
        }
    }
}
