package io.axoniq.axonserver.eventstore.transformation.requestprocessor;

import io.axoniq.axonserver.grpc.event.Event;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.util.function.Function;

import static java.lang.String.format;


/**
 * Implementation of {@link ContextTransformer} that guarantees that all actions are performed sequentially.
 */
public class SequentialContextTransformer implements ContextTransformer {

    private final String context;
    private final TransformationApplier applier;
    private final TransformationRollbackExecutor rollbackExecutor;

    private final ContextTransformationStore store;
    private final TransformationStateConverter converter;
    private final Sinks.Many<Mono<?>> taskExecutor = Sinks.many()
                                                          .unicast()
                                                          .onBackpressureBuffer();


    public SequentialContextTransformer(String context,
                                        ContextTransformationStore store,
                                        TransformationStateConverter converter,
                                        TransformationApplier applier,
                                        TransformationRollbackExecutor rollbackExecutor) {
        this.applier = applier;
        this.rollbackExecutor = rollbackExecutor;
        this.context = context;
        this.store = store;
        this.converter = converter;
        startListening();
    }

    private void startListening() {
        taskExecutor.asFlux()
                    .concatMap(Function.identity())
                    .subscribe();
    }

    @Override
    public Mono<Void> restart() {
        return store.current()
                    .flatMap(transformation -> {
                        if (EventStoreTransformationJpa.Status.APPLYING.equals(transformation.status())) {
                            return apply(transformation);
                        } else if (EventStoreTransformationJpa.Status.ROLLING_BACK.equals(transformation.status())) {
                            return rollback(transformation);
                        }
                        return Mono.empty();
                    });
    }


    @Override
    public Mono<String> start(String description) {
        return store.create()
                    .map(TransformationState::id)
                    .as(this::sequential);
    }

    @Override
    public Mono<Void> deleteEvent(String transformationId, long tokenToDelete, long sequence) {
        return perform(transformationId,
                       "DELETE_EVENT",
                       transformation -> transformation.deleteEvent(tokenToDelete, sequence))
                .then();
    }

    @Override
    public Mono<Void> replaceEvent(String transformationId, long token, Event event, long sequence) {
        return perform(transformationId,
                       "REPLACE_EVENT",
                       transformation -> transformation.replaceEvent(token, event, sequence)).then();
    }

    @Override
    public Mono<Void> cancel(String transformationId) {
        return perform(transformationId, "CANCEL_TRANSFORMATION", Transformation::cancel).then();
    }

    @Override
    public Mono<Void> startApplying(String transformationId, long sequence, boolean keepOldVersions) {
        return perform(transformationId,
                       "START_APPLYING_TRANSFORMATION",
                       transformation -> transformation.startApplying(sequence, keepOldVersions))
                .then();
    }

    @Override
    public Mono<Void> apply(Transformation transformation) {
        return applier.apply(new TransformationApplier.Transformation() {
        }).then(
                perform(transformation.id(),
                       "MARK_AS_APPLIED",
                       Transformation::markApplied))

                .then();
    }

    @Override
    public Mono<Void> startRollingBack(String transformationId) {
        return perform(transformationId,
                       "START_ROLLING_BACK_TRANSFORMATION",
                       Transformation::startRollingBack)
                .flatMap(this::rollback);
    }

    @Override
    public Mono<Void> markRolledBack(String transformationId) {
        return perform(state.id(),
                                             "MARK_AS_ROLLED_BACK",
                                             Transformation::markRolledBack))
                               .then();
    }

    @Override
    public Mono<Void> deleteOldVersions() {
        return Mono.<Void>fromRunnable(() -> {
        }).as(this::sequential);
    }

    private Mono<TransformationState> perform(String transformationId,
                                              String actionName,
                                              Function<Transformation, Mono<TransformationState>> action) {
        return store.transformation(transformationId) // state interface (implemented by jpa) #with(...)
                    .flatMap(converter::from)
                    .checkpoint(format("Starting %s action", actionName))
                    .flatMap(action)
                    .checkpoint(format("Action %s completed", actionName))
                    .flatMap(state -> store.save(state)
                                           .thenReturn(state))
                    .checkpoint(format("Transformation updated after %s", actionName))
                    .as(this::sequential);
    }

    private <R> Mono<R> sequential(Mono<R> action) {
        return Mono.deferContextual(contextView -> {
            Sinks.One<R> actionResult = Sinks.one();
            while (taskExecutor.tryEmitNext(action.doOnError(t -> actionResult.emitError(t,
                                                                                         Sinks.EmitFailureHandler.FAIL_FAST))
                                                  .doOnSuccess(next -> actionResult.emitValue(next,
                                                                                              Sinks.EmitFailureHandler.FAIL_FAST)))
                    != Sinks.EmitResult.OK) {
            }
            return actionResult
                    .asMono()
                    .contextWrite(contextView);
        });
    }
}

class RollbackTransformation implements TransformationRollbackExecutor.Transformation {

    private final String context;
    private final TransformationState state;

    RollbackTransformation(String context, TransformationState state) {
        this.context = context;
        this.state = state;
    }

    @Override
    public String id() {
        return state.id();
    }

    @Override
    public int version() {
        return state.version();
    }

    @Override
    public String context() {
        return context;
    }

    @Override
    public String toString() {
        return "RollbackTransformation{" +
                "context='" + context + '\'' +
                ", state=" + state +
                '}';
    }
}




