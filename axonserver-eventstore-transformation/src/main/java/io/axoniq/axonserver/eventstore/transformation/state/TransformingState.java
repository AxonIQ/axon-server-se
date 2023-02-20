package io.axoniq.axonserver.eventstore.transformation.state;

import io.axoniq.axonserver.eventstore.transformation.requestprocessor.EventStoreStateStore;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.EventStoreStateStore.EventStoreState;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.WrongTransformationStateException;

/**
 * Represents the state of a context's event store when there is a transformation in progress.
 * That means that the events are going to be changed.
 *
 * @author Milan Savic
 * @author Sara Pellegrini
 * @since 2023.0.0
 */
public class TransformingState implements EventStoreState {

    private final String transformationId;
    private final String context;

    /**
     * Constructs an instance for the specified operation identifier and context.
     *
     * @param transformationId the identifier of the transformation
     * @param context          the context
     */
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
    public EventStoreState transformed() {
        return new IdleState(context);
    }

    @Override
    public EventStoreState cancelled() {
        return new IdleState(context);
    }

    @Override
    public EventStoreState transform(String transformationId) {
        throw new WrongTransformationStateException("There is already ongoing transformation");
    }
}
