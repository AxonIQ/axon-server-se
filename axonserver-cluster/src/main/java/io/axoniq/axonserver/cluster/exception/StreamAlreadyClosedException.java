package io.axoniq.axonserver.cluster.exception;

/**
 * Exception thrown when peer tries to send message to a connection that has already been closed.
 *
 * @author Marc Gathier
 * @since 4.2
 */
public class StreamAlreadyClosedException extends RuntimeException {

    public StreamAlreadyClosedException(Throwable e) {
        super(e);
    }
}
