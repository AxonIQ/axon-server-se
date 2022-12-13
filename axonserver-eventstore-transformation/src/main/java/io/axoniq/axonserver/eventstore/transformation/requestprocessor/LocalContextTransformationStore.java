package io.axoniq.axonserver.eventstore.transformation.requestprocessor;

import io.axoniq.axonserver.eventstore.transformation.jpa.EventStoreTransformationJpa;
import io.axoniq.axonserver.eventstore.transformation.jpa.EventStoreTransformationRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.Date;

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
    public Flux<TransformationState> transformations() {
        return Flux.fromStream(() -> repository.findAll()
                                               .stream())
                   .subscribeOn(Schedulers.boundedElastic())
                   .map(DefaultTransformationState::new);
    }

    @Override
    public Mono<Void> create(String id, String description) {
        return lastAppliedTransformation()//TODO applied or last transformation?
                                          .map(lastVersion -> new EventStoreTransformationJpa(id,
                                                                                              description,
                                                                                              context,
                                                                                              lastVersion + 1))
                                          .map(repository::save)
                                          .subscribeOn(Schedulers.boundedElastic())
                                          .then();
    }

    @Override
    public Mono<TransformationState> transformation(String id) {
        return Mono.fromSupplier(() -> repository.findById(id))
                   .subscribeOn(Schedulers.boundedElastic())
                   .flatMap(Mono::justOrEmpty)
                   .map(DefaultTransformationState::new);
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
        state.applier()
             .ifPresent(jpa::setApplier);
        state.appliedAt()
             .ifPresent(appliedAt -> jpa.setDateApplied(Date.from(appliedAt)));
        state.lastEventToken()
             .ifPresent(jpa::setLastEventToken);
        return jpa;
    }
}

