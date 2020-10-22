package io.axoniq.axonserver.grpc.heartbeat;

/**
 * Exception used any time Axon Server detects a client inactivity in a long living bidirectional stream.
 *
 * @author Sara Pellegrini
 * @since 4.3.9, 4.4.4
 */
public class ApplicationInactivityException extends Exception {

    /**
     * Constructs a new {@link ApplicationInactivityException} without message.
     */
    public ApplicationInactivityException() {
    }

    /**
     * Constructs a new {@link ApplicationInactivityException} with the specified message.
     *
     * @param message a detailed message describing the cause for this exception
     */
    public ApplicationInactivityException(String message) {
        super(message);
    }
}
