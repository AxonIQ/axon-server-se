package io.axoniq.axonserver.cluster.exception;

/**
 * @author Sara Pellegrini
 * @since 4.0
 */
public class LeadershipTransferInProgressException extends RuntimeException {

    public LeadershipTransferInProgressException(String message) {
        super(message);
    }

    public LeadershipTransferInProgressException(String message, Throwable cause) {
        super(message, cause);
    }
}
