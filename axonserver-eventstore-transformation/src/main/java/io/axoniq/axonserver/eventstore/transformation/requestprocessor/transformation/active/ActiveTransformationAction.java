package io.axoniq.axonserver.eventstore.transformation.requestprocessor.transformation.active;

import io.axoniq.axonserver.eventstore.transformation.TransformationAction;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.TransformationState;
import reactor.core.publisher.Mono;

import java.util.Map;

interface ActiveTransformationAction {

    /**
     *
     * @return last sequence
     */
    Mono<TransformationAction> apply(); //TODO return an interface i.o. data object

}
