package io.axoniq.axonserver.eventstore.transformation.transformation.active;

import io.axoniq.axonserver.eventstore.transformation.ReplaceEvent;
import io.axoniq.axonserver.eventstore.transformation.TransformationAction;
import io.axoniq.axonserver.grpc.event.Event;
import reactor.core.publisher.Mono;

import static java.lang.String.format;

public class ReplaceEventAction implements ActiveTransformationAction {

    private final long tokenToReplace;
    private final Event replacement;
    private final TransformationResources resources;

    public ReplaceEventAction(long tokenToReplace,
                              Event replacement,
                              TransformationResources resources) {
        this.tokenToReplace = tokenToReplace;
        this.replacement = replacement;
        this.resources = resources;
    }

    @Override
    public Mono<TransformationAction> apply() {
        return validateEvent()
                .then(Mono.fromSupplier(this::action));
    }

    private Mono<Event> validateEvent() {
        return resources.event(tokenToReplace)
                        .flatMap(this::validateAggregateSequenceNumber)
                        .flatMap(this::validateAggregateIdentifier)
                        .switchIfEmpty(Mono.error(new RuntimeException("Event not found: " + tokenToReplace)));
    }


    private Mono<Event> validateAggregateSequenceNumber(Event event) {
        if (event.getAggregateSequenceNumber() == replacement.getAggregateSequenceNumber()) {
            return Mono.just(event);
        }
        return Mono.error(new RuntimeException(format("Invalid aggregate sequence number: %d, expecting %d",
                                                      replacement.getAggregateSequenceNumber(),
                                                      event.getAggregateSequenceNumber())));
    }

    private Mono<Event> validateAggregateIdentifier(Event event) {
        if (event.getAggregateIdentifier().equals(replacement.getAggregateIdentifier())) {
            return Mono.just(event);
        }
        return Mono.error(new RuntimeException(format("Invalid aggregate identifier: %s, expecting %s",
                                                      replacement.getAggregateIdentifier(),
                                                      event.getAggregateIdentifier())));
    }

    private TransformationAction action() {
        ReplaceEvent replaceEvent = ReplaceEvent.newBuilder()
                                                .setToken(tokenToReplace)
                                                .setEvent(replacement)
                                                .build();
        return TransformationAction.newBuilder()
                                   .setReplaceEvent(replaceEvent)
                                   .build();
    }
}

