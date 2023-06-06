package io.axoniq.axonserver.eventstore.transformation.clean.transformations;

import io.axoniq.axonserver.eventstore.transformation.clean.TransformationIdentifier;
import io.axoniq.axonserver.eventstore.transformation.clean.TransformationsToBeCleaned;
import io.axoniq.axonserver.eventstore.transformation.jpa.EventStoreTransformationRepository;
import reactor.core.publisher.Flux;

import java.util.Optional;

import static io.axoniq.axonserver.eventstore.transformation.jpa.EventStoreTransformationJpa.Status.APPLIED;

/**
 * @author Sara Pellegrini
 * @since 2023.0.0
 */
public class AppliedTransformations implements TransformationsToBeCleaned {

    private final TransformationsToBeCleaned localTransformationStores;

    private final EventStoreTransformationRepository repository;

    public AppliedTransformations(TransformationsToBeCleaned localTransformationStores,
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
                                        .filter(entity -> APPLIED.equals(entity.status()))
                                        .map(JpaTransformationIdentifier::new);
    }
}
