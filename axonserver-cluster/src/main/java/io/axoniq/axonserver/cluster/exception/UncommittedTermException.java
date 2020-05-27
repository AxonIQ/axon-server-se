package io.axoniq.axonserver.cluster.exception;

/**
 * Exception thrown when it is not possible to accept a configuration change request because the committed term is
 * older than current term.
 *
 * @author Sara Pellegrini
 * @since 4.4
 */
public class UncommittedTermException extends RuntimeException {

    public UncommittedTermException(String message) {
        super(message);
    }

    public UncommittedTermException(String message, Throwable cause) {
        super(message, cause);
    }
}
