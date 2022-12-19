package io.axoniq.axonserver.eventstore.transformation.state;

import io.axoniq.axonserver.eventstore.transformation.requestprocessor.EventStoreStateStore;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.EventStoreStateStore.EventStoreState;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.WrongTransformationStateException;
import reactor.core.publisher.Mono;

/**
 * @author Sara Pellegrini
 * @since 2023.0.0
 */
public class TransformingState implements EventStoreState {

    private final String transformationId;
    private final String context;

    public TransformingState(String transformationId, String context) {
        this.transformationId = transformationId;
        this.context = context;
    }

    @Override
    public void accept(EventStoreStateStore.Visitor visitor) {
        visitor.setContext(context)
               .setOperationId(transformationId)
               .setState(EventStoreStateStore.State.TRANSFORMING);
    }

    @Override
    public Mono<EventStoreState> transformed() {
        return Mono.fromSupplier(() -> new IdleState(context));
    }

    @Override
    public Mono<EventStoreState> transform(String transformationId) {
        return Mono.error(new WrongTransformationStateException("There is already ongoing transformation"));
    }
}
