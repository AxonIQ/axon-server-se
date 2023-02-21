package io.axoniq.axonserver.eventstore.transformation.state;

import io.axoniq.axonserver.eventstore.transformation.requestprocessor.EventStoreStateStore;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.EventStoreStateStore.EventStoreState;

/**
 * Represents the state of a context's event store when there is no operation in progress.
 * That means that it is possible to request a new operation execution.
 *
 * @author Milan Savic
 * @author Sara Pellegrini
 * @since 2023.0.0
 */
public class IdleState implements EventStoreState {

    private final String context;

    /**
     * Constructs an instance for the specified context.
     *
     * @param context the context
     */
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
    public EventStoreState transform(String transformationId) {
        return new TransformingState(transformationId, context);
    }

    @Override
    public EventStoreState compact(String compactionId) {
        return new CompactingState(compactionId, context);
    }
}
