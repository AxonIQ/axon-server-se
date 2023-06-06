package io.axoniq.axonserver.eventstore.transformation.requestprocessor;

import io.axoniq.axonserver.eventstore.transformation.jpa.EventStoreTransformationJpa;
import io.axoniq.axonserver.eventstore.transformation.jpa.EventStoreTransformationRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.Date;

public class LocalContextTransformationStore implements ContextTransformationStore {

    private static final Logger logger = LoggerFactory.getLogger(LocalContextTransformationStore.class);

    private final String context;
    private final EventStoreTransformationRepository repository;
    private final TransformationEntryStoreProvider transformationEntryStoreSupplier;

    public LocalContextTransformationStore(String context,
                                           EventStoreTransformationRepository repository,
                                           TransformationEntryStoreProvider transformationEntryStoreSupplier) {
        this.context = context;
        this.repository = repository;
        this.transformationEntryStoreSupplier = transformationEntryStoreSupplier;
    }

    @Override
    public Flux<TransformationState> transformations() {
        return Flux.fromStream(() -> repository.findAll()
                                               .stream())
                   .subscribeOn(Schedulers.boundedElastic())
                   .map(DefaultTransformationState::new);
    }

    @Override
    public void create(String id, String description) {
        EventStoreTransformationJpa entity = new EventStoreTransformationJpa(id,
                                                                             description,
                                                                             context,
                                                                             lastTransformationVersion() + 1);
        repository.save(entity);
    }

    @Override
    public Mono<TransformationState> transformation(String id) {
        return Mono.fromSupplier(() -> repository.findById(id))
                   .doFirst(() -> logger.info("Finding transformation with id {}.", id))
                   .subscribeOn(Schedulers.boundedElastic())
                   .flatMap(Mono::justOrEmpty)
                   .doOnSuccess(s -> logger.info("Found transformation with id {}.", id))
                   .map(DefaultTransformationState::new);
    }

    private int lastTransformationVersion() {
        return repository.lastVersion(context)
                         .orElse(0);
    }

    @Override
    public void save(TransformationState transformation) {
        storeStagedActions(transformation);
        repository.save(entity(transformation));
    }

    @Override
    public void clean(String context) {
        repository.deleteAllByContext(context);
    }

    private void storeStagedActions(TransformationState transformation) {
        transformationEntryStoreSupplier.provide(context, transformation.id())
                                        .flatMapMany(transformationEntryStore -> Flux.fromIterable(transformation.staged())
                                                                                     .flatMap(
                                                                                             transformationEntryStore::store))
                                        .then()
                                        .block();
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

