package io.axoniq.axonserver.enterprise.task;

/**
 * @author Marc Gathier
 */
public class TransientException extends RuntimeException {

    public TransientException(String message) {
        super(message);
    }

    public TransientException(String message, Throwable cause) {
        super(message, cause);
    }
}
