package io.axoniq.axonserver.eventstore.transformation.requestprocessor.transformation.active;

import io.axoniq.axonserver.eventstore.transformation.TransformationAction;
import reactor.core.publisher.Mono;

interface ActiveTransformationAction {

    /**
     *
     * @return last sequence
     */
    Mono<TransformationAction> apply(); //TODO return an interface i.o. data object

}
