package io.axoniq.axonserver.eventstore.transformation.clean.transformations;

import io.axoniq.axonserver.eventstore.transformation.clean.TransformationIdentifier;
import io.axoniq.axonserver.eventstore.transformation.clean.TransformationsToBeCleaned;
import io.axoniq.axonserver.eventstore.transformation.jpa.EventStoreTransformationRepository;
import reactor.core.publisher.Flux;

/**
 * @author Sara Pellegrini
 * @since 2023.0.0
 */
public class MissingTransformations implements TransformationsToBeCleaned {

    private final TransformationsToBeCleaned localTransformationStores;
    private final EventStoreTransformationRepository repository;

    public MissingTransformations(FileSystemTransformations localTransformationStores,
                                  EventStoreTransformationRepository repository) {
        this.localTransformationStores = localTransformationStores;
        this.repository = repository;
    }

    @Override
    public Flux<TransformationIdentifier> get() {
        return localTransformationStores.get()
                                        .filter(transformation -> !repository.existsById(transformation.id()));
    }
}
