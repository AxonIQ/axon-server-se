package io.axoniq.axonserver.eventstore.transformation.jpa;

import io.axoniq.axonserver.eventstore.transformation.requestprocessor.EventStoreStateStore;
import io.axoniq.axonserver.eventstore.transformation.state.CompactingState;
import io.axoniq.axonserver.eventstore.transformation.state.IdleState;
import io.axoniq.axonserver.eventstore.transformation.state.TransformingState;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Instant;
import java.util.EnumMap;

/**
 * @author Sara Pellegrini
 * @since 2023.0.0
 */
public class JpaEventStoreStateStore implements EventStoreStateStore {

    private final JpaEventStoreStateRepo repository;


    public JpaEventStoreStateStore(JpaEventStoreStateRepo repository) {
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
            io.axoniq.axonserver.eventstore.transformation.jpa.EventStoreState entity) {
        switch (entity.getState()) {
            case IDLE:
                return new IdleState(entity.getContext());
            case TRANSFORMING:
                return new TransformingState(entity.getContext());
            case COMPACTING:
                return new CompactingState(entity.getContext());
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

        private static final EnumMap<State, io.axoniq.axonserver.eventstore.transformation.jpa.EventStoreState.State> stateMapping = new EnumMap<>(
                State.class);
        private final io.axoniq.axonserver.eventstore.transformation.jpa.EventStoreState entity = new io.axoniq.axonserver.eventstore.transformation.jpa.EventStoreState();

        @Override
        public Visitor setContext(String context) {
            entity.setContext(context);
            return this;
        }

        @Override
        public Visitor setState(State state) {
            io.axoniq.axonserver.eventstore.transformation.jpa.EventStoreState.State s = stateMapping.get(state);
            entity.setState(s);
            return this;
        }

        io.axoniq.axonserver.eventstore.transformation.jpa.EventStoreState entity() {
            entity.setLastUpdate(Instant.now());
            return entity;
        }

        static {
            stateMapping.put(State.IDLE, io.axoniq.axonserver.eventstore.transformation.jpa.EventStoreState.State.IDLE);
            stateMapping.put(State.COMPACTING,
                             io.axoniq.axonserver.eventstore.transformation.jpa.EventStoreState.State.COMPACTING);
            stateMapping.put(State.TRANSFORMING,
                             io.axoniq.axonserver.eventstore.transformation.jpa.EventStoreState.State.TRANSFORMING);
        }
    }
}
