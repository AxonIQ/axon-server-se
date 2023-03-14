package io.axoniq.axonserver.eventstore.transformation.jpa;

import io.axoniq.axonserver.eventstore.transformation.api.EventStoreTransformationService.Transformation;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.Transformations;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Instant;
import java.util.Date;
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
        return jpaEntity.transformationId();
    }

    @Override
    public String context() {
        return jpaEntity.context();
    }

    @Override
    public String description() {
        return jpaEntity.description();
    }

    @Override
    public int version() {
        return jpaEntity.version();
    }

    @Override
    public Optional<Long> lastSequence() {
        return Optional.ofNullable(jpaEntity.lastSequence());
    }

    @Override
    public Status status() {
        switch (jpaEntity.status()) {
            case APPLIED:
                return Status.APPLIED;
            case ACTIVE:
                return Status.ACTIVE;
            case APPLYING:
                return Status.APPLYING;
            case CANCELLED:
                return Status.CANCELLED;
            default:
                throw new IllegalStateException("Unsupported transformation status");
        }
    }

    @Override
    public Optional<String> applyRequester() {
        return Optional.ofNullable(jpaEntity.applier());
    }

    @Override
    public Optional<Instant> appliedAt() {
        return Optional.ofNullable(jpaEntity.dateApplied())
                       .map(Date::toInstant);
    }
}