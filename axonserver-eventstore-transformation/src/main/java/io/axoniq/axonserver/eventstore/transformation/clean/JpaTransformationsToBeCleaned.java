package io.axoniq.axonserver.eventstore.transformation.clean;

import io.axoniq.axonserver.eventstore.transformation.jpa.EventStoreTransformationJpa;
import io.axoniq.axonserver.eventstore.transformation.jpa.EventStoreTransformationRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import static io.axoniq.axonserver.eventstore.transformation.jpa.EventStoreTransformationJpa.Status.APPLIED;
import static io.axoniq.axonserver.eventstore.transformation.jpa.EventStoreTransformationJpa.Status.CANCELLED;
import static java.util.Arrays.asList;

public class JpaTransformationsToBeCleaned implements TransformationsToBeCleaned {

    private final EventStoreTransformationRepository repository;

    public JpaTransformationsToBeCleaned(EventStoreTransformationRepository repository) {
        this.repository = repository;
    }

    @Override
    public Flux<TransformationToBeCleaned> toBeCleaned() {
        return Flux.defer(() -> Flux.fromIterable(repository.findAll())
                                    .subscribeOn(Schedulers.boundedElastic())
                                    .filter(entity -> asList(APPLIED, CANCELLED).contains(entity.status()))
                                    .filter(entity -> !entity.cleaned())
                                    .map(t -> new Transformation(repository, t)));
    }

    private static class Transformation implements TransformationToBeCleaned {

        private final EventStoreTransformationRepository repo;
        private final EventStoreTransformationJpa entity;

        private Transformation(EventStoreTransformationRepository repo, EventStoreTransformationJpa entity) {
            this.repo = repo;
            this.entity = entity;
        }

        @Override
        public String context() {
            return entity.context();
        }

        @Override
        public String id() {
            return entity.transformationId();
        }

        @Override
        public Mono<Void> markAsCleaned() {
            return Mono.<Void>fromRunnable(() -> repo.markAsCleaned(id()))
                       .subscribeOn(Schedulers.boundedElastic());
        }
    }
}
