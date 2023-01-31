package io.axoniq.axonserver.eventstore.transformation.requestprocessor;

import io.axoniq.axonserver.eventstore.transformation.api.EventStoreTransformationService.Transformation;
import reactor.core.publisher.Flux;

import static io.axoniq.axonserver.eventstore.transformation.api.EventStoreTransformationService.Transformation.Status.*;
import static java.util.Arrays.asList;

public interface Transformations {

    Flux<Transformation> allTransformations();

    default Flux<Transformation> currentTransformations() {
        return allTransformations()
                .filter(transformation -> asList(ACTIVE, APPLYING).contains(transformation.status()));
    }

    default Flux<Transformation> applyingTransformations() {
        return currentTransformations().filter(t -> APPLYING.equals(t.status()));
    }
}
