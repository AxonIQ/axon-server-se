package io.axoniq.axonserver.eventstore.transformation.jpa;

import io.axoniq.axonserver.eventstore.transformation.requestprocessor.EventStoreStateStore;
import io.axoniq.axonserver.eventstore.transformation.state.CompactingState;
import io.axoniq.axonserver.eventstore.transformation.state.IdleState;
import io.axoniq.axonserver.eventstore.transformation.state.TransformingState;

import java.util.EnumMap;

/**
 * JPA implementation of the {@link EventStoreStateStore}.
 *
 * @author Sara Pellegrini
 * @author Milan Savic
 * @since 2023.0.0
 */
public class JpaEventStoreStateStore implements EventStoreStateStore {

    private final EventStoreStateRepository repository;


    /**
     * Create an instance that uses the specified JPA repository.
     *
     * @param repository the JPA repository for handling the persistence.
     */
    public JpaEventStoreStateStore(EventStoreStateRepository repository) {
        this.repository = repository;
    }

    @Override
    public EventStoreState state(String context) {
        return repository.findById(context)
                         .map(this::from)
                         .orElse(new IdleState(context));
    }

    private EventStoreState from(
            EventStoreStateJpa entity) {
        switch (entity.state()) {
            case IDLE:
                return new IdleState(entity.context());
            case TRANSFORMING:
                return new TransformingState(entity.inProgressOperationId(), entity.context());
            case COMPACTING:
                return new CompactingState(entity.inProgressOperationId(), entity.context());
            default:
                throw new IllegalStateException("");
        }
    }

    @Override
    public void save(EventStoreState state) {
        JpaEntityConstructor entityConstructor = new JpaEntityConstructor();
        state.accept(entityConstructor);
        repository.save(entityConstructor.entity());
    }

    @Override
    public void clean(String context) {
        if (repository.existsById(context)) {
            repository.deleteById(context);
        }
    }

    private static class JpaEntityConstructor implements Visitor {

        private static final EnumMap<State, EventStoreStateJpa.State> stateMapping = new EnumMap<>(
                State.class);
        private final EventStoreStateJpa entity = new EventStoreStateJpa();

        @Override
        public Visitor setContext(String context) {
            entity.setContext(context);
            return this;
        }

        @Override
        public Visitor setState(State state) {
            EventStoreStateJpa.State s = stateMapping.get(state);
            entity.setState(s);
            return this;
        }

        @Override
        public Visitor setOperationId(String operationId) {
            entity.setInProgressOperationId(operationId);
            return this;
        }

        EventStoreStateJpa entity() {
            return entity;
        }

        static {
            stateMapping.put(State.IDLE, EventStoreStateJpa.State.IDLE);
            stateMapping.put(State.COMPACTING,
                             EventStoreStateJpa.State.COMPACTING);
            stateMapping.put(State.TRANSFORMING,
                             EventStoreStateJpa.State.TRANSFORMING);
        }
    }
}
