package io.axoniq.axonserver.eventstore.transformation.clean.transformations;

import io.axoniq.axonserver.eventstore.transformation.clean.TransformationIdentifier;
import io.axoniq.axonserver.eventstore.transformation.clean.TransformationsToBeCleaned;
import reactor.core.publisher.Flux;

/**
 * @author Sara Pellegrini
 * @since 2023.0.0
 */
public class MultipleTransformations implements TransformationsToBeCleaned {

    private final Iterable<TransformationsToBeCleaned> transformationsArray;

    public MultipleTransformations(Iterable<TransformationsToBeCleaned> transformationsArray) {
        this.transformationsArray = transformationsArray;
    }

    @Override
    public Flux<TransformationIdentifier> get() {
        return Flux.fromIterable(transformationsArray)
                   .flatMap(TransformationsToBeCleaned::get);
    }
}
