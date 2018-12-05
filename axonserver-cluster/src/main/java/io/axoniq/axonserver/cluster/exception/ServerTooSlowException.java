package io.axoniq.axonserver.cluster.exception;

/**
 * @author Sara Pellegrini
 * @since 4.0
 */
public class ServerTooSlowException extends RuntimeException {

    public ServerTooSlowException(String message) {
        super(message);
    }

    public ServerTooSlowException(String message, Throwable cause) {
        super(message, cause);
    }
}
