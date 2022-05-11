package io.axoniq.axonserver.eventstore.transformation.requestprocessor;

import io.axoniq.axonserver.grpc.event.Event;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.util.function.Function;

import static java.lang.String.format;


/**
 * Implementation of {@link ContextTransformer} that guarantees that all actions are performed sequentially.
 */
public class SequentialContextTransformer implements ContextTransformer {

    private final String context;
    private final ContextTransformationStore store;
    private final TransformationStateConverter converter;
    private final Sinks.Many<Mono<?>> taskExecutor = Sinks.many()
                                                          .unicast()
                                                          .onBackpressureBuffer();


    public SequentialContextTransformer(String context,
                                        ContextTransformationStore store,
                                        TransformationStateConverter converter) {
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

    private Mono<TransformationState> ongoingTransformation() {
        return store.transformations()
                    .filter(TransformationState::ongoing)
                    .next();
    }

    @Override
    public Mono<String> start(String description) {
        return ongoingTransformation()
                .<String>flatMap(transformation -> Mono.error(new RuntimeException("There is already ongoing transformation")))
                .switchIfEmpty(store.create()
                                    .map(TransformationState::id)
                                    .as(this::sequential));
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
    public Mono<Void> startCancelling(String transformationId) {
        return perform(transformationId, "CANCEL_TRANSFORMATION", Transformation::startCancelling).then();
    }

    @Override
    public Mono<Void> markAsCancelled(String transformationId) {
        return perform(transformationId,
                       "MARK_AS_CANCELLED",
                       Transformation::markCancelled);
    }


    @Override
    public Mono<Void> startApplying(String transformationId, long sequence) {
        return perform(transformationId,
                       "START_APPLYING_TRANSFORMATION",
                       transformation -> transformation.startApplying(sequence));
    }

    @Override
    public Mono<Void> markApplied(String transformationId) {
        return perform(transformationId,
                       "MARK_AS_APPLIED",
                       Transformation::markApplied);
    }

    @Override
    public Mono<Void> startRollingBack(String transformationId) {
        return perform(transformationId,
                       "START_ROLLING_BACK_TRANSFORMATION",
                       Transformation::startRollingBack);
    }

    @Override
    public Mono<Void> markRolledBack(String transformationId) {
        return perform(transformationId,
                       "MARK_AS_ROLLED_BACK",
                       Transformation::markRolledBack);
    }

    @Override
    public Mono<Void> deleteOldVersions() {
        return Mono.<Void>fromRunnable(() -> {// TODO: 5/11/22 !!!
        }).as(this::sequential);
    }

    private Mono<Void> perform(String transformationId,
                               String actionName,
                               Function<Transformation, Mono<TransformationState>> action) {
        return store.transformation(transformationId) // state interface (implemented by jpa) #with(...)
                    .flatMap(converter::from)
                    .checkpoint(format("Starting %s action", actionName))
                    .flatMap(action)
                    .checkpoint(format("Action %s completed", actionName))
                    .flatMap(store::save)
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