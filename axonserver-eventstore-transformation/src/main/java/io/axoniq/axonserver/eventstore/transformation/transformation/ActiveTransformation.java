package io.axoniq.axonserver.eventstore.transformation.transformation;

import io.axoniq.axonserver.eventstore.transformation.requestprocessor.Transformation;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.TransformationState;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.WrongTransformationStateException;
import io.axoniq.axonserver.eventstore.transformation.transformation.active.ActiveTransformationAction;
import io.axoniq.axonserver.eventstore.transformation.transformation.active.DeleteEventAction;
import io.axoniq.axonserver.eventstore.transformation.transformation.active.ReplaceEventAction;
import io.axoniq.axonserver.grpc.event.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import static io.axoniq.axonserver.eventstore.transformation.jpa.EventStoreTransformationJpa.Status.CANCELLED;
import static java.lang.String.format;


public class ActiveTransformation implements Transformation {

    private static final Logger logger = LoggerFactory.getLogger(ActiveTransformation.class);
    private final TransformationState state;

    public ActiveTransformation(TransformationState state) {
        this.state = state;
    }

    @Override
    public Mono<TransformationState> deleteEvent(long tokenToDelete, long sequence) {
        DeleteEventAction action = new DeleteEventAction(tokenToDelete);
        return performEventAction(action, sequence, tokenToDelete)
                .doFirst(() -> logger.trace("Storing delete event action with token: {}", tokenToDelete))
                .doOnSuccess(s -> logger.trace("Delete event action with token: {} stored", tokenToDelete))
                .doOnError(e -> logger.warn("Failure storing delete event action with token: {}", tokenToDelete, e));
    }

    @Override
    public Mono<TransformationState> replaceEvent(long token, Event event, long sequence) {
        ReplaceEventAction action = new ReplaceEventAction(token, event);
        return performEventAction(action, sequence, token)
                .doOnSuccess(s -> logger.trace("Replace event action with token: {} stored", token))
                .doOnError(e -> logger.warn("Failure storing replace event action with token: {}", token, e));
    }

    @Override
    public Mono<TransformationState> cancel() {
        return Mono.fromSupplier(() -> state.withStatus(CANCELLED));
    }

    @Override
    public Mono<TransformationState> startApplying(long sequence, String applier) {
        return Mono.defer(() -> Mono.justOrEmpty(state.lastSequence()))
                   .switchIfEmpty(Mono.error(new WrongTransformationStateException(
                           "Cannot apply an empty transformation")))
                   .map(lastSequence -> lastSequence == sequence)
                   .filter(valid -> valid)
                   .switchIfEmpty(Mono.error(new WrongTransformationStateException("Invalid sequence " + sequence))) // TODO: 2/2/23 add expected sequence
                   .map(notUsed -> state.applying(applier));
    }

    private Mono<TransformationState> performEventAction(ActiveTransformationAction action, long sequence, long token) {
        return validateSequence(sequence).then(validateEventsOrder(token)
                                                       .then(action.apply())
                                                       .map(state::stage)
                                                       .map(s -> s.withLastEventToken(token)));
    }

    private Mono<Void> validateEventsOrder(long token) {
        return state.lastEventToken()
                    .map(lastToken -> lastToken
                            < token ? Mono.<Void>empty() : Mono.<Void>error(new WrongTransformationStateException(
                            format("The token [%d] is lower or equals than last modified token [%d] of this transformation.",
                                   token,
                                   lastToken))))
                    .orElse(Mono.empty());
    }

    private Mono<Void> validateSequence(long sequence) {
        return state.lastSequence()
                    .map(lastSequence -> lastSequence + 1 == sequence ? Mono.<Void>empty() :
                            Mono.<Void>error(new WrongTransformationStateException(format(
                                    "The sequence [%d] is different from the expected one [%d]",
                                    sequence,
                                    lastSequence + 1))))
                    .orElse(Mono.empty());
    }
}