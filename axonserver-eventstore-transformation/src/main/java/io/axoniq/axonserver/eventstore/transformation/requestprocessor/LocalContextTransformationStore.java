package io.axoniq.axonserver.eventstore.transformation.requestprocessor;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.Date;
import java.util.UUID;

public class LocalContextTransformationStore implements ContextTransformationStore {

    private final String context;
    private final EventStoreTransformationRepository repository;
    private final TransformationEntryStore transformationEntryStore;

    public LocalContextTransformationStore(String context,
                                           EventStoreTransformationRepository repository,
                                           TransformationEntryStore transformationEntryStore) {
        this.context = context;
        this.repository = repository;
        this.transformationEntryStore = transformationEntryStore;
    }

    @Override
    public Mono<TransformationState> create() {
        return lastAppliedTransformation()
                .map(lastVersion -> repository.save(new EventStoreTransformationJpa(UUID.randomUUID().toString(),
                                                                                    context,
                                                                                    lastVersion + 1)))
                .subscribeOn(Schedulers.boundedElastic())
                .map(JpaTransformationState::new);
    }

    @Override
    public Mono<TransformationState> transformation(String id) {
        return Mono.fromSupplier(() -> repository.findById(id))
                   .subscribeOn(Schedulers.boundedElastic())
                   .flatMap(Mono::justOrEmpty)
                   .map(JpaTransformationState::new);
    }

    private Mono<Integer> lastAppliedTransformation() {
        return Mono.fromSupplier(() -> repository.lastAppliedVersion(context)
                                                 .orElse(0))
                   .subscribeOn(Schedulers.boundedElastic());
    }

    @Override
    public Mono<Void> save(TransformationState transformation) {
        return Flux.fromIterable(transformation.staged())
                   .flatMap(transformationEntryStore::store)
                   .then(Mono.fromSupplier(() -> repository.save(entity(transformation)))
                             .subscribeOn(Schedulers.boundedElastic())
                             .then());
    }

    private EventStoreTransformationJpa entity(TransformationState state) {
        EventStoreTransformationJpa jpa = new EventStoreTransformationJpa();
        jpa.setTransformationId(state.id());
        jpa.setContext(context);
        jpa.setDescription(state.description());
        jpa.setStatus(state.status());
        state.lastSequence()
             .ifPresent(jpa::setLastSequence);
        jpa.setVersion(state.version());
        state.applied()
             .ifPresent(applied -> {
                 jpa.setAppliedBy(applied.by());
                 jpa.setDateApplied(Date.from(applied.at()));
             });
        return jpa;
    }
}

