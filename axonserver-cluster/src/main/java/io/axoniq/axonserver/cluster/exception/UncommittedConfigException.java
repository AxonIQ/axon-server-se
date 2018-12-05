package io.axoniq.axonserver.cluster.exception;

/**
 * @author Sara Pellegrini
 * @since 4.0
 */
public class UncommittedConfigException extends RuntimeException {

    public UncommittedConfigException(String message) {
        super(message);
    }

    public UncommittedConfigException(String message, Throwable cause) {
        super(message, cause);
    }
}
