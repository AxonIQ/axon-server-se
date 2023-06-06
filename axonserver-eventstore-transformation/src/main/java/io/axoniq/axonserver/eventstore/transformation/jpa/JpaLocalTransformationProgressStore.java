package io.axoniq.axonserver.eventstore.transformation.jpa;

import io.axoniq.axonserver.eventstore.transformation.apply.TransformationApplyingState;
import io.axoniq.axonserver.eventstore.transformation.apply.TransformationProgressStore;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.Optional;

public class JpaLocalTransformationProgressStore implements TransformationProgressStore {

    private final LocalEventStoreTransformationRepository repository;

    public JpaLocalTransformationProgressStore(LocalEventStoreTransformationRepository repository) {
        this.repository = repository;
    }

    @Override
    public Mono<TransformationApplyingState> initState(String transformationId) {
        return Mono.just(new LocalEventStoreTransformationJpa(transformationId, -1, false))
                   .doOnNext(repository::save)
                   .map(JpaTransformationApplyingState::new);
    }

    @Override
    public Mono<TransformationApplyingState> stateFor(String transformationId) {
        return Mono.<TransformationApplyingState>create(sink -> {
            Optional<LocalEventStoreTransformationJpa> byId = repository.findById(transformationId);
            if (byId.isPresent()) {
                sink.success(new JpaTransformationApplyingState(byId.get()));
            } else {
                sink.success();
            }
        }).subscribeOn(Schedulers.boundedElastic());
    }

    @Override
    public Mono<Void> incrementLastSequence(String transformationId, long sequenceIncrement) {
        return Mono.<Void>fromRunnable(() -> repository.incrementLastSequence(transformationId, sequenceIncrement))
                   .subscribeOn(Schedulers.boundedElastic());
    }

    @Override
    public Mono<Void> markAsApplied(String transformationId) {
        return stateFor(transformationId).map(state -> new LocalEventStoreTransformationJpa(transformationId,
                                                                                            state.lastAppliedSequence(),
                                                                                            true))
                                         .doOnNext(repository::save)
                                         .then();
    }

    @Override
    public Mono<Void> clean() {
        return Mono.<Void>fromRunnable(repository::deleteOrphans)
                   .subscribeOn(Schedulers.boundedElastic());
    }

    private static class JpaTransformationApplyingState implements TransformationApplyingState {

        private final LocalEventStoreTransformationJpa entity;

        private JpaTransformationApplyingState(LocalEventStoreTransformationJpa entity) {
            this.entity = entity;
        }

        @Override
        public long lastAppliedSequence() {
            return entity.getLastSequenceApplied();
        }

        @Override
        public boolean applied() {
            return entity.isApplied();
        }
    }
}
