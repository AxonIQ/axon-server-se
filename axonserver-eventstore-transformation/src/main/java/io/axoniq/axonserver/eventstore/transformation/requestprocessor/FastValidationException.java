package io.axoniq.axonserver.eventstore.transformation.requestprocessor;

public class FastValidationException extends RuntimeException {

    public FastValidationException() {
        super();
    }

    public FastValidationException(String message) {
        super(message);
    }

    public FastValidationException(String message, Throwable cause) {
        super(message, cause);
    }
}
