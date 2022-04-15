package io.axoniq.axonserver.eventstore.transformation.requestprocessor;

import io.axoniq.axonserver.eventstore.transformation.api.EventStoreTransformationService.Transformation;
import reactor.core.publisher.Flux;

import java.util.List;

import static io.axoniq.axonserver.eventstore.transformation.api.EventStoreTransformationService.Transformation.Status.*;
import static java.util.Arrays.asList;

public interface Transformations {

    List<Transformation.Status> CURRENT_STATUSES = asList(ACTIVE, APPLYING);

    Flux<Transformation> allTransformations();

    default Flux<Transformation> currentTransformations() {
        return allTransformations()
                .filter(transformation -> CURRENT_STATUSES.contains(transformation.status()));
    }

    default Flux<Transformation> applyingTransformations() {
        return currentTransformations().filter(t -> APPLYING.equals(t.status()));
    }

    default Flux<Transformation> rollingBackTransformations() {
        return allTransformations().filter(t -> ROLLING_BACK.equals(t.status()));
    }
}
