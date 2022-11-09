package io.axoniq.axonserver.eventstore.transformation.transformation.active;

import io.axoniq.axonserver.eventstore.transformation.DeleteEvent;
import io.axoniq.axonserver.eventstore.transformation.TransformationAction;
import reactor.core.publisher.Mono;

public class DeleteEventAction implements ActiveTransformationAction {

    private final long tokenToDelete;
    private final TransformationResources resources;

    public DeleteEventAction(long tokenToDelete,
                             TransformationResources resources) {
        this.tokenToDelete = tokenToDelete;
        this.resources = resources;
    }

    @Override
    public Mono<TransformationAction> apply() {
        return validateEvent()
                .checkpoint("Event with token " + tokenToDelete + " validated for deletion ")
                .then(Mono.fromSupplier(this::action));
    }


    private Mono<Void> validateEvent() {
        return resources.event(tokenToDelete)
                        .switchIfEmpty(Mono.error(new RuntimeException("")))
                        .then();
    }

    private TransformationAction action() {
        DeleteEvent deleteEvent = DeleteEvent.newBuilder()
                                             .setToken(tokenToDelete)
                                             .build();
        return TransformationAction.newBuilder()
                                   .setDeleteEvent(deleteEvent)
                                   .build();
    }
}