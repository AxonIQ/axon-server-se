package io.axoniq.axonserver.eventstore.transformation.state;

import io.axoniq.axonserver.eventstore.transformation.requestprocessor.EventStoreStateStore;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.EventStoreStateStore.EventStoreState;
import reactor.core.publisher.Mono;

/**
 * @author Sara Pellegrini
 * @since 2023.0.0
 */
public class TransformingState implements EventStoreState {

    private final String context;

    public TransformingState(String context) {
        this.context = context;
    }

    @Override
    public void accept(EventStoreStateStore.Visitor visitor) {
        visitor.setContext(context)
               .setState(EventStoreStateStore.State.TRANSFORMING);
    }

    @Override
    public Mono<EventStoreState> transformed() {
        return Mono.fromSupplier(() -> new IdleState(context));
    }

    @Override
    public Mono<EventStoreState> transform() {
        return Mono.error(new IllegalStateException("There is already ongoing transformation"));
    }
}
