package io.axoniq.axonserver.eventstore.transformation.jpa;

import io.axoniq.axonserver.eventstore.transformation.requestprocessor.EventStoreStateStore;
import io.axoniq.axonserver.eventstore.transformation.state.CompactingState;
import io.axoniq.axonserver.eventstore.transformation.state.IdleState;
import io.axoniq.axonserver.eventstore.transformation.state.TransformingState;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.EnumMap;

/**
 * @author Sara Pellegrini
 * @since 2023.0.0
 */
public class JpaEventStoreStateStore implements EventStoreStateStore {

    private final EventStoreStateRepository repository;


    public JpaEventStoreStateStore(EventStoreStateRepository repository) {
        this.repository = repository;
    }

    @Override
    public Mono<EventStoreState> state(String context) {
        return Mono.defer(() -> Mono.justOrEmpty(repository.findById(context))
                                    .subscribeOn(Schedulers.boundedElastic())
                                    .map(this::from)
                                    .switchIfEmpty(Mono.just(new IdleState(context))));
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
    public Mono<Void> save(EventStoreState state) {
        return Mono.<Void>fromRunnable(() -> {
            JpaEntityConstructor entityConstructor = new JpaEntityConstructor();
            state.accept(entityConstructor);
            repository.save(entityConstructor.entity());
        }).subscribeOn(Schedulers.boundedElastic());
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
