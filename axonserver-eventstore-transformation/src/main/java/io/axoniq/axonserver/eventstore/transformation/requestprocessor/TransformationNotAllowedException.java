package io.axoniq.axonserver.eventstore.transformation.requestprocessor;

public class TransformationNotAllowedException extends IllegalStateException {

    public TransformationNotAllowedException() {
        super();
    }

    public TransformationNotAllowedException(String s) {
        super(s);
    }
}
