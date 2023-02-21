package io.axoniq.axonserver.eventstore.transformation.state;

import io.axoniq.axonserver.eventstore.transformation.requestprocessor.EventStoreStateStore;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.EventStoreStateStore.EventStoreState;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.WrongTransformationStateException;

/**
 * Represents the state of a context's event store when there is a compaction operation in progress.
 * That means that the obsoleted versions of the events are going to be deleted.
 *
 * @author Milan Savic
 * @author Sara Pellegrini
 * @since 2023.0.0
 */
public class CompactingState implements EventStoreState {

    private final String compactionId;
    private final String context;

    /**
     * Constructs an instance for the specified operation identifier and context.
     *
     * @param compactionId the identifier of the compaction operation
     * @param context      the context
     */
    public CompactingState(String compactionId, String context) {
        this.context = context;
        this.compactionId = compactionId;
    }

    @Override
    public void accept(EventStoreStateStore.Visitor visitor) {
        visitor.setContext(context)
               .setState(EventStoreStateStore.State.COMPACTING)
               .setOperationId(compactionId);
    }

    @Override
    public EventStoreState compacted() {
        return new IdleState(context);
    }

    @Override
    public EventStoreState transform(String transformationId) {
        throw new WrongTransformationStateException("Cannot start a new transformation during compaction.");
    }
}
