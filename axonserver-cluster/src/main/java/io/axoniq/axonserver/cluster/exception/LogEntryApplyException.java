package io.axoniq.axonserver.cluster.exception;

/**
 * @author Marc Gathier
 */
public class LogEntryApplyException extends RuntimeException {

    public LogEntryApplyException(String message, Throwable cause) {
        super(message, cause);
    }
}
