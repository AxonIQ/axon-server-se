package io.axoniq.axonserver.eventstore.transformation.requestprocessor.transformation.active;

import io.axoniq.axonserver.eventstore.transformation.requestprocessor.Transformation;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.TransformationResources;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.TransformationState;
import io.axoniq.axonserver.grpc.event.Event;
import reactor.core.publisher.Mono;

import static io.axoniq.axonserver.eventstore.transformation.requestprocessor.EventStoreTransformationJpa.Status.APPLYING;
import static io.axoniq.axonserver.eventstore.transformation.requestprocessor.EventStoreTransformationJpa.Status.CANCELLED;


public class ActiveTransformation implements Transformation {

    private final TransformationResources resources;
    private final TransformationState state;

    public ActiveTransformation(TransformationResources resources, TransformationState state) {
        this.resources = resources;
        this.state = state;
    }

    @Override
    public Mono<TransformationState> deleteEvent(long tokenToDelete, long sequence) {
        DeleteEventAction action = new DeleteEventAction(tokenToDelete, resources);
        return performAction(action, sequence);
    }

    @Override
    public Mono<TransformationState> replaceEvent(long token, Event event, long sequence) {
        ReplaceEventAction action = new ReplaceEventAction(token, event, resources);
        return performAction(action, sequence);
    }

    @Override
    public Mono<TransformationState> cancel() {
        return Mono.fromSupplier(() -> state.withStatus(CANCELLED));
    }

    @Override
    public Mono<TransformationState> startApplying(long sequence) {
        boolean valid = state.lastSequence()
                .map(lastSequence -> lastSequence == sequence)
                .orElse(true);

        return valid ? Mono.just(state.withStatus(APPLYING)) : Mono.error(new RuntimeException("Invalid sequence"));
    }

    private Mono<TransformationState> performAction(ActiveTransformationAction action, long token) {
        return validateChain(token)
                .flatMap(v -> action.apply())
                .map(state::stage);
    }

    private Mono<Void> validateChain(long token) {
        return state.lastEventToken()
                    .map(lastToken -> lastToken < token ? Mono.<Void>empty() : Mono.<Void>error(new RuntimeException(
                            "The token [%s] of the action for token doesn't match %s d")))
                    .orElse(Mono.empty());
    }
}