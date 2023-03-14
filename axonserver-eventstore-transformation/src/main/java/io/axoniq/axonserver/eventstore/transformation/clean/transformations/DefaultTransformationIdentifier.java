package io.axoniq.axonserver.eventstore.transformation.clean.transformations;

import io.axoniq.axonserver.eventstore.transformation.clean.TransformationIdentifier;

/**
 * @author Sara Pellegrini
 * @since 2023.0.0
 */
public class DefaultTransformationIdentifier implements TransformationIdentifier {

    private final String transformationId;
    private final String context;

    public DefaultTransformationIdentifier(String context, String transformationId) {
        this.transformationId = transformationId;
        this.context = context;
    }

    @Override
    public String context() {
        return context;
    }

    @Override
    public String id() {
        return transformationId;
    }
}
