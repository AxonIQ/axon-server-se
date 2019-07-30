package io.axoniq.axonserver.cluster.exception;

/**
 * Exception thrown when the apply of a log entry fails for an unexpected reason.
 *
 * @author Marc Gathier
 * @since 4.1.7
 */
public class LogEntryApplyException extends RuntimeException {

    public LogEntryApplyException(String message, Throwable cause) {
        super(message, cause);
    }
}
