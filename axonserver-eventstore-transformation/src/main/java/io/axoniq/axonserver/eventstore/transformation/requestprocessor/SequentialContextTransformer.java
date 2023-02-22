package io.axoniq.axonserver.eventstore.transformation.requestprocessor;

import io.axoniq.axonserver.grpc.event.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.util.function.Function;


/**
 * Implementation of {@link ContextTransformer} that guarantees that all actions are performed sequentially.
 *
 * @author Sara Pellegrini
 * @author Milan Savic
 * @since 2023.0.0
 */
public class SequentialContextTransformer implements ContextTransformer {

    private static final Logger logger = LoggerFactory.getLogger(SequentialContextTransformer.class);

    private final ContextTransformer delegate;

    private final Sinks.Many<Mono<?>> taskExecutor = Sinks.many()
                                                          .unicast()
                                                          .onBackpressureBuffer();


    /**
     * Create an instance that delegates the execution of operation to the delegate {@link ContextTransformer}.
     *
     * @param delegate the {@link ContextTransformer} the operations will be delegated to
     */
    SequentialContextTransformer(ContextTransformer delegate) {
        this.delegate = delegate;
        startListening();
    }

    private void startListening() {
        taskExecutor.asFlux()
                    .concatMap(Function.identity())
                    .onErrorContinue((error, o) -> {
                    })
                    .subscribe();
    }

    @Override
    public Mono<Void> start(String id, String description) {
        return delegate.start(id, description)
                       .as(this::sequential);
    }

    @Transactional


    @Override
    public Mono<Void> deleteEvent(String transformationId, long tokenToDelete, long sequence) {
        logger.info("Invoking delete event pipeline");
        return delegate.deleteEvent(transformationId, tokenToDelete, sequence)
                       .as(this::sequential);
    }

    @Override
    public Mono<Void> replaceEvent(String transformationId, long token, Event event, long sequence) {
        return delegate.replaceEvent(transformationId, token, event, sequence)
                       .as(this::sequential);
    }

    @Override
    public Mono<Void> cancel(String transformationId) {
        return delegate.cancel(transformationId)
                       .as(this::sequential);
    }


    @Override
    public Mono<Void> startApplying(String transformationId, long sequence, String applier) {
        return delegate.startApplying(transformationId, sequence, applier)
                       .as(this::sequential);
    }

    @Override
    public Mono<Void> markApplied(String transformationId) {
        return delegate.markApplied(transformationId)
                       .as(this::sequential);
    }

    @Override
    public Mono<Void> markCompacted(String compactionId) {
        return delegate.markCompacted(compactionId)
                       .as(this::sequential);
    }

    @Override
    public Mono<Void> clean() {
        return delegate.clean()
                       .as(this::sequential);
    }

    @Override
    public Mono<Void> startCompacting(String compactionId) {
        return delegate.startCompacting(compactionId)
                       .as(this::sequential);
    }


    private <R> Mono<R> sequential(Mono<R> action) {// TODO: 09/11/2022 rethink sequentializing options
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