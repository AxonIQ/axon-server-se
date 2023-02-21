package io.axoniq.axonserver.eventstore.transformation.requestprocessor;

import io.axoniq.axonserver.eventstore.transformation.NonTransientTransformationException;

public class TransformationNotAllowedException extends IllegalStateException implements
        NonTransientTransformationException {

    public TransformationNotAllowedException() {
        super();
    }

    public TransformationNotAllowedException(String s) {
        super(s);
    }
}
