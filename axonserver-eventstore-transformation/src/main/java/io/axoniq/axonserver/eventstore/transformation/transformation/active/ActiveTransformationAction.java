package io.axoniq.axonserver.eventstore.transformation.transformation.active;

import io.axoniq.axonserver.eventstore.transformation.TransformationAction;
import reactor.core.publisher.Mono;

public interface ActiveTransformationAction {

    /**
     * @return last sequence
     */
    Mono<TransformationAction> apply(); //TODO return an interface i.o. data object
}
