package io.axoniq.axonserver.eventstore.transformation.requestprocessor;

import io.axoniq.axonserver.eventstore.transformation.NonTransientTransformationException;

/**
 * @author Milan Savic
 * @author Sara Pellegrini
 * @since 2023.0.0
 */
public class WrongTransformationStateException extends IllegalStateException implements
        NonTransientTransformationException {

    public WrongTransformationStateException() {
    }

    public WrongTransformationStateException(String s) {
        super(s);
    }
}
