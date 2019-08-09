package io.axoniq.axonserver.cluster.exception;

/**
 * @author Marc Gathier
 */
public class ConcurrentMembershipStateModificationException extends RuntimeException {

    public ConcurrentMembershipStateModificationException(String message) {
        super(message);
    }
}
