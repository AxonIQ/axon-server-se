package io.axoniq.axonserver.eventstore.transformation.requestprocessor;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Stores the state of the event transformations for a specific context.
 *
 * @author Milan Savic
 * @author Sara Pellegrini
 * @since 2023.0.0
 */
public interface ContextTransformationStore {

    /**
     * Returns the {@link Mono} of the {@link TransformationState} with the specified identifier.
     *
     * @param id the identifier of the transformation to be retrieved.
     * @return the {@link Mono} of the {@link TransformationState} with the specified identifier.
     */
    Mono<TransformationState> transformation(String id);

    /**
     * Returns the {@link Flux} of all the {@link TransformationState} for the context.
     *
     * @return the {@link Flux} of all the {@link TransformationState} for the context.
     */
    Flux<TransformationState> transformations();

    /**
     * Creates and save a new the {@link TransformationState} with specified identifier and description.
     *
     * @param id          the identifier of the new transformation
     * @param description the description of the new transformation
     */
    void create(String id, String description);

    /**
     * Save a the {@link TransformationState} provided as parameter
     *
     * @param transformation the {@link TransformationState} to be saved.
     */
    void save(TransformationState transformation);

    void clean(String context);
}
