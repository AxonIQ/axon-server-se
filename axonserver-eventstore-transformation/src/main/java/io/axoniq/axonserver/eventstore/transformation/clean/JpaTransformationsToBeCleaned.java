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
    private final CleanedTransformationRepository cleanedTransformationRepository;

    public JpaTransformationsToBeCleaned(EventStoreTransformationRepository repository,
                                         CleanedTransformationRepository cleanedTransformationRepository) {
        this.repository = repository;
        this.cleanedTransformationRepository = cleanedTransformationRepository;
    }

    @Override
    public Flux<TransformationToBeCleaned> toBeCleaned() {
        return Flux.defer(() -> Flux.fromIterable(repository.findAll())
                                    .subscribeOn(Schedulers.boundedElastic())
                                    .filter(entity -> asList(APPLIED, CANCELLED).contains(entity.status()))
                                    .filter(entity -> !cleanedTransformationRepository.existsById(entity.transformationId()))
                                    .map(t -> new Transformation(cleanedTransformationRepository, t)));
    }

    private static class Transformation implements TransformationToBeCleaned {

        private final CleanedTransformationRepository repo;
        private final EventStoreTransformationJpa entity;

        private Transformation(CleanedTransformationRepository repo, EventStoreTransformationJpa entity) {
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
            return Mono.<Void>fromRunnable(() -> repo.save(new CleanedTransformationJpa(id())))
                       .subscribeOn(Schedulers.boundedElastic());
        }
    }
}
