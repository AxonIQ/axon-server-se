package io.axoniq.axonserver.eventstore.transformation.transformation.active;

import io.axoniq.axonserver.eventstore.transformation.ReplaceEvent;
import io.axoniq.axonserver.eventstore.transformation.TransformationAction;
import io.axoniq.axonserver.grpc.event.Event;
import reactor.core.publisher.Mono;

public class ReplaceEventAction implements ActiveTransformationAction {

    private final long tokenToReplace;
    private final Event replacement;

    public ReplaceEventAction(long tokenToReplace,
                              Event replacement) {
        this.tokenToReplace = tokenToReplace;
        this.replacement = replacement;
    }

    @Override
    public Mono<TransformationAction> apply() {
        return Mono.fromSupplier(this::action);
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

