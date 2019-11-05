package io.axoniq.axonserver.enterprise.task;

/**
 * Exception to be thrown when a task fails with an error that may disappear by itself (or by restarting a component).
 *
 * @author Marc Gathier
 * @since 4.3
 */
public class TransientException extends RuntimeException {

    public TransientException(String message) {
        super(message);
    }

    public TransientException(String message, Throwable cause) {
        super(message, cause);
    }
}
