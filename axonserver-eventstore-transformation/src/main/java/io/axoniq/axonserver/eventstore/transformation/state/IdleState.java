package io.axoniq.axonserver.eventstore.transformation.state;

import io.axoniq.axonserver.eventstore.transformation.requestprocessor.EventStoreStateStore;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.EventStoreStateStore.EventStoreState;
import reactor.core.publisher.Mono;

/**
 * @author Sara Pellegrini
 * @since 2023.0.0
 */
public class IdleState implements EventStoreState {

    private final String context;

    public IdleState(String context) {
        this.context = context;
    }

    @Override
    public void accept(EventStoreStateStore.Visitor visitor) {
        visitor.setContext(context)
               .setState(EventStoreStateStore.State.IDLE)
               .setOperationId(null);
    }

    @Override
    public Mono<EventStoreState> transform(String transformationId) {
        return Mono.fromSupplier(() -> new TransformingState(transformationId, context));
    }

    @Override
    public Mono<EventStoreState> compact(String compactionId) {
        return Mono.fromSupplier(() -> new CompactingState(compactionId, context));
    }
}
