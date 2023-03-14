package io.axoniq.axonserver.eventstore.transformation.clean.transformations;

import io.axoniq.axonserver.eventstore.transformation.clean.TransformationIdentifier;
import io.axoniq.axonserver.eventstore.transformation.clean.TransformationsToBeCleaned;
import io.axoniq.axonserver.eventstore.transformation.jpa.EventStoreTransformationRepository;
import reactor.core.publisher.Flux;

import java.util.Optional;

import static io.axoniq.axonserver.eventstore.transformation.jpa.EventStoreTransformationJpa.Status.CANCELLED;

/**
 * @author Sara Pellegrini
 * @since 2023.0.0
 */
public class CancelledTransformations implements TransformationsToBeCleaned {

    private final TransformationsToBeCleaned localTransformationStores;

    private final EventStoreTransformationRepository repository;

    public CancelledTransformations(TransformationsToBeCleaned localTransformationStores,
                                    EventStoreTransformationRepository repository) {
        this.localTransformationStores = localTransformationStores;
        this.repository = repository;
    }

    @Override
    public Flux<TransformationIdentifier> get() {
        return localTransformationStores.get()
                                        .map(TransformationIdentifier::id)
                                        .filter(repository::existsById)
                                        .map(repository::findById)
                                        .map(Optional::get)
                                        .filter(entity -> CANCELLED.equals(entity.status()))
                                        .map(JpaTransformationIdentifier::new);
    }
}
