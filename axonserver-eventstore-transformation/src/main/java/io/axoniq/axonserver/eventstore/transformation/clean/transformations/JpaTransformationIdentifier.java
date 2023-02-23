package io.axoniq.axonserver.eventstore.transformation.clean.transformations;

import io.axoniq.axonserver.eventstore.transformation.clean.TransformationIdentifier;
import io.axoniq.axonserver.eventstore.transformation.jpa.EventStoreTransformationJpa;

/**
 * @author Sara Pellegrini
 * @since 2023.0.0
 */
public class JpaTransformationIdentifier implements TransformationIdentifier {

    private final EventStoreTransformationJpa entity;

    public JpaTransformationIdentifier(EventStoreTransformationJpa entity) {
        this.entity = entity;
    }

    @Override
    public String context() {
        return entity.context();
    }

    @Override
    public String id() {
        return entity.transformationId();
    }
}
