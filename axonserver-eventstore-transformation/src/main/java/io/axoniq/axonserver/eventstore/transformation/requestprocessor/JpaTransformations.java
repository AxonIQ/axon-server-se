package io.axoniq.axonserver.eventstore.transformation.requestprocessor;

import io.axoniq.axonserver.eventstore.transformation.api.EventStoreTransformationService.Transformation;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.Optional;

public class JpaTransformations implements Transformations {

    private final EventStoreTransformationRepository repository;

    public JpaTransformations(EventStoreTransformationRepository repository) {
        this.repository = repository;
    }

    @Override
    public Flux<Transformation> allTransformations() {
        return Mono.fromSupplier(repository::findAll)
                   .subscribeOn(Schedulers.boundedElastic())
                   .flatMapMany(Flux::fromIterable)
                   .map(JPATransformation::new);
    }
}

class JPATransformation implements Transformation {


    private final EventStoreTransformationJpa jpaEntity;

    JPATransformation(EventStoreTransformationJpa jpaEntity) {
        this.jpaEntity = jpaEntity;
    }

    @Override
    public String id() {
        return jpaEntity.getTransformationId();
    }

    @Override
    public String context() {
        return jpaEntity.getContext();
    }

    @Override
    public int version() {
        return jpaEntity.getVersion();
    }

    @Override
    public Optional<Long> lastSequence() {
        return Optional.ofNullable(jpaEntity.getLastSequence());
    }

    @Override
    public Status status() {
        // TODO: 4/15/22
        return null;
    }
}