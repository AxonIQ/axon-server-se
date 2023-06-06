package io.axoniq.axonserver.eventstore.transformation.jpa;

import io.axoniq.axonserver.eventstore.transformation.spi.TransformationsInProgressForContext;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class JpaTransformationsInProgressForContext implements TransformationsInProgressForContext {

    private final EventStoreStateRepository repository;

    public JpaTransformationsInProgressForContext(EventStoreStateRepository repository) {
        this.repository = repository;
    }

    @Override
    public Mono<Boolean> inProgress(String context) {
        return Mono.fromSupplier(() -> repository.findById(context))
                   .subscribeOn(Schedulers.boundedElastic())
                   .map(optionalEventStoreState -> optionalEventStoreState.map(this::notIdle)
                                                                          .orElse(false));
    }

    private boolean notIdle(EventStoreStateJpa state) {
        return !EventStoreStateJpa.State.IDLE.equals(state.state());
    }
}
