package io.axoniq.axonserver.localstorage.transformation;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.Optional;

public class JpaLocalTransformationProgressStore implements LocalTransformationProgressStore {

    private final EventStoreTransformationProgressRepository repository;

    public JpaLocalTransformationProgressStore(EventStoreTransformationProgressRepository repository) {
        this.repository = repository;
    }

    @Override
    public Mono<TransformationApplyingState> initState(String transformationId) {
        return Mono.just(new EventStoreTransformationProgressJpa(transformationId, -1, false))
                   .doOnNext(repository::save)
                   .map(JpaTransformationApplyingState::new);
    }

    @Override
    public Mono<TransformationApplyingState> stateFor(String transformationId) {
        return Mono.<TransformationApplyingState>create(sink -> {
            Optional<EventStoreTransformationProgressJpa> byId = repository.findById(transformationId);
            if (byId.isPresent()) {
                sink.success(new JpaTransformationApplyingState(byId.get()));
            } else {
                sink.success();
            }
        }).subscribeOn(Schedulers.boundedElastic());
    }

    @Override
    public Mono<Void> updateLastSequence(String transformationId, long lastProcessedSequence) {
        return stateFor(transformationId).filter(state -> !state.applied())
                                         .switchIfEmpty(Mono.error(new RuntimeException("unexisting transformation")))
                                         .map(state -> new EventStoreTransformationProgressJpa(transformationId,
                                                                                               lastProcessedSequence,
                                                                                               state.applied()))
                                         .doOnNext(repository::save)
                                         .then();
    }

    @Override
    public Mono<Void> markAsApplied(String transformationId) {
        return stateFor(transformationId).map(state -> new EventStoreTransformationProgressJpa(transformationId,
                                                                                               state.lastAppliedSequence(),
                                                                                               true))
                                         .doOnNext(repository::save)
                                         .then();
    }

    private static class JpaTransformationApplyingState implements TransformationApplyingState {

        private final EventStoreTransformationProgressJpa entity;

        private JpaTransformationApplyingState(EventStoreTransformationProgressJpa entity) {
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
