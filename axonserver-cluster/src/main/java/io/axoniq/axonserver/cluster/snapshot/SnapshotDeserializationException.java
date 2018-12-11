package io.axoniq.axonserver.cluster.snapshot;

/**
 * @author Milan Savic
 */
public class SnapshotDeserializationException extends RuntimeException {

    public SnapshotDeserializationException(String message, Throwable cause) {
        super(message, cause);
    }
}
