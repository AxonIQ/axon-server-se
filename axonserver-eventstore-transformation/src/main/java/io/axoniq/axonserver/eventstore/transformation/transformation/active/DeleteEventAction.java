package io.axoniq.axonserver.eventstore.transformation.transformation.active;

import io.axoniq.axonserver.eventstore.transformation.DeleteEvent;
import io.axoniq.axonserver.eventstore.transformation.TransformationAction;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.WrongTransformationStateException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

public class DeleteEventAction implements ActiveTransformationAction {

    private static final Logger logger = LoggerFactory.getLogger(DeleteEventAction.class);

    private final long tokenToDelete;

    public DeleteEventAction(long tokenToDelete) {
        this.tokenToDelete = tokenToDelete;
    }

    @Override
    public Mono<TransformationAction> apply() {
        return Mono.fromSupplier(this::action)
                   .doFirst(() -> logger.info("Applying DELETE EVENT action."))
                   .doOnSuccess(a -> logger.info("Applied DELETE EVENT action."));
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