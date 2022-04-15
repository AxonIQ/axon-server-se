package io.axoniq.axonserver.eventstore.transformation.requestprocessor.transformation.active;

import io.axoniq.axonserver.eventstore.transformation.DeleteEvent;
import io.axoniq.axonserver.eventstore.transformation.TransformationAction;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.ProtoTransformationEntry;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.TransformationEntry;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.TransformationResources;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.TransformationState;
import reactor.core.publisher.Mono;

class DeleteEventAction implements ActiveTransformationAction {

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