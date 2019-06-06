package io.axoniq.axonserver.cluster.exception;

/**
 * This exception is thrown when it is no longer possible to monitor the progress of the replication due to an
 * interruption of the replication process.
 * This can happen, for example, when a new node is added to the cluster and during the initial update, the leader
 * changes.
 *
 * @author Sara Pellegrini
 * @since 4.1.1
 */
public class ReplicationInterruptedException extends RuntimeException {

    public ReplicationInterruptedException(String message) {
        super(message);
    }

    public ReplicationInterruptedException(String message, Throwable cause) {
        super(message, cause);
    }
}
