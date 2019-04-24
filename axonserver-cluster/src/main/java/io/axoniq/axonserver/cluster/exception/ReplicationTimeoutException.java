package io.axoniq.axonserver.cluster.exception;

/**
 * This exception is thrown when it is no longer possible to monitor the progress of the replication due to a timeout.
 * This can happen, for example, when a new node is added to the cluster and during the initial update, the _admin changes.
 *
 * @author Sara Pellegrini
 * @since 4.1.1
 */
public class ReplicationTimeoutException extends RuntimeException{

    public ReplicationTimeoutException(String message) {
        super(message);
    }

    public ReplicationTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }
}
