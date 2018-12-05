package io.axoniq.axonserver.cluster.exception;

/**
 * @author Sara Pellegrini
 * @since 4.0
 */
public class ServerNotReadyException extends RuntimeException{

    public ServerNotReadyException(String message) {
        super(message);
    }

    public ServerNotReadyException(String message, Throwable cause) {
        super(message, cause);
    }
}
