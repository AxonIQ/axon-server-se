package io.axoniq.axonserver.eventstore.transformation.requestprocessor.transformation.active;

import io.axoniq.axonserver.eventstore.transformation.requestprocessor.Transformation;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.TransformationResources;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.TransformationState;
import io.axoniq.axonserver.grpc.event.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import static io.axoniq.axonserver.eventstore.transformation.requestprocessor.EventStoreTransformationJpa.Status.*;
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
                .doOnSuccess(s -> logger.trace("Replacing event with token: {}", tokenToDelete));
    }

    @Override
    public Mono<TransformationState> replaceEvent(long token, Event event, long sequence) {
        ReplaceEventAction action = new ReplaceEventAction(token, event, resources);
        return performEventAction(action, sequence, token)
                .doOnSuccess(s -> logger.trace("Replacing event with token: {}", token));
    }

    @Override
    public Mono<TransformationState> startCancelling() {
        return Mono.fromSupplier(() -> state.withStatus(CANCELLING));
    }

    @Override
    public Mono<TransformationState> markCancelled() {
        return Mono.fromSupplier(() -> state.withStatus(CANCELLED));
    }

    @Override
    public Mono<TransformationState> startApplying(long sequence) {
        boolean valid = state.lastSequence()
                             .map(lastSequence -> lastSequence == sequence)
                             .orElse(true);

        return valid ? Mono.just(state.withStatus(APPLYING)) : Mono.error(new RuntimeException("Invalid sequence"));
    }

    private Mono<TransformationState> performEventAction(ActiveTransformationAction action, long sequence, long token) {
        return validateSequence(sequence).then(validateEventsOrder(token)
                                                       .then(action.apply())
                                                       .map(state::stage)
                                                       .map(state -> state.withLastEventToken(token)));
    }

    private Mono<Void> validateEventsOrder(long token) {
        return state.lastEventToken()
                    .map(lastToken -> lastToken < token ? Mono.<Void>empty() : Mono.<Void>error(new RuntimeException(
                            format("The token [%d] is lower or equals than last modified token [%d] of this transformation.", token, lastToken))))
                    .orElse(Mono.empty());
    }

    private Mono<Void> validateSequence(long sequence) {
        return Mono.empty();// TODO: 5/11/22 !!!
    }
}