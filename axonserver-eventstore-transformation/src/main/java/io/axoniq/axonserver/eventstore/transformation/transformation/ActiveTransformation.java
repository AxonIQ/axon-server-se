package io.axoniq.axonserver.eventstore.transformation.transformation;

import io.axoniq.axonserver.eventstore.transformation.requestprocessor.Transformation;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.TransformationState;
import io.axoniq.axonserver.eventstore.transformation.transformation.active.ActiveTransformationAction;
import io.axoniq.axonserver.eventstore.transformation.transformation.active.DeleteEventAction;
import io.axoniq.axonserver.eventstore.transformation.transformation.active.ReplaceEventAction;
import io.axoniq.axonserver.eventstore.transformation.transformation.active.TransformationResources;
import io.axoniq.axonserver.grpc.event.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import static io.axoniq.axonserver.eventstore.transformation.jpa.EventStoreTransformationJpa.Status.CANCELLING;
import static java.lang.String.format;


public class ActiveTransformation implements Transformation {

    private static final Logger logger = LoggerFactory.getLogger(ActiveTransformation.class);
    private final TransformationResources resources;
    private final TransformationState state;

    public ActiveTransformation(TransformationResources resources, TransformationState state) {
        this.resources = resources;
        this.state = state;
    }

    @Override
    public Mono<TransformationState> deleteEvent(long tokenToDelete, long sequence) {
        DeleteEventAction action = new DeleteEventAction(tokenToDelete, resources);
        return performEventAction(action, sequence, tokenToDelete)
                .doOnSuccess(s -> logger.trace("Deleting event with token: {}", tokenToDelete))
                .doOnError(e -> logger.warn("Failure deleting event with token: {}", tokenToDelete, e));
    }

    @Override
    public Mono<TransformationState> replaceEvent(long token, Event event, long sequence) {
        ReplaceEventAction action = new ReplaceEventAction(token, event, resources);
        return performEventAction(action, sequence, token)
                .doOnSuccess(s -> logger.trace("Replacing event with token: {}", token))
                .doOnError(e -> logger.warn("Failure replacing event with token: {}", token, e));
    }

    @Override
    public Mono<TransformationState> startCancelling() {
        return Mono.fromSupplier(() -> state.withStatus(CANCELLING))
                   .flatMap(s -> resources.close().thenReturn(s));
    }

    @Override
    public Mono<TransformationState> startApplying(long sequence, String applier) {
        return Mono.defer(() -> Mono.justOrEmpty(state.lastSequence()))
                   .switchIfEmpty(Mono.error(new RuntimeException("Cannot apply an empty transformation")))
                   .map(lastSequence -> lastSequence == sequence)
                   .filter(valid -> valid)
                   .switchIfEmpty(Mono.error(new RuntimeException("Invalid sequence")))
                   .map(notUsed -> state.applying(applier))
                   .flatMap(s -> resources.close().thenReturn(s));
    }

    private Mono<TransformationState> performEventAction(ActiveTransformationAction action, long sequence, long token) {
        return validateSequence(sequence).then(validateEventsOrder(token)
                                                       .then(action.apply())
                                                       .map(state::stage)
                                                       .map(s -> s.withLastEventToken(token)));
    }

    private Mono<Void> validateEventsOrder(long token) {
        return state.lastEventToken()
                    .map(lastToken -> lastToken < token ? Mono.<Void>empty() : Mono.<Void>error(new RuntimeException(
                            format("The token [%d] is lower or equals than last modified token [%d] of this transformation.",
                                   token,
                                   lastToken))))
                    .orElse(Mono.empty());
    }

    private Mono<Void> validateSequence(long sequence) {
        return state.lastSequence()
                    .map(lastSequence -> lastSequence + 1 == sequence ? Mono.<Void>empty() :
                            Mono.<Void>error(new RuntimeException(format(
                                    "The sequence [%d] is different from the expected one [%d]",
                                    sequence,
                                    lastSequence + 1))))
                    .orElse(Mono.empty());
    }
}