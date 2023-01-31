package io.axoniq.axonserver.eventstore.transformation.state;

import io.axoniq.axonserver.eventstore.transformation.requestprocessor.EventStoreStateStore;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.EventStoreStateStore.EventStoreState;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.WrongTransformationStateException;
import reactor.core.publisher.Mono;

/**
 * @author Sara Pellegrini
 * @since 2023.0.0
 */
public class CompactingState implements EventStoreState {

    private final String compactionId;
    private final String context;

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
    public Mono<EventStoreState> compacted() {
        return Mono.fromSupplier(() -> new IdleState(context));
    }

    @Override
    public Mono<EventStoreState> transform(String transformationId) {
        return Mono.error(new WrongTransformationStateException("Cannot start a new transformation during compaction."));
    }
}
