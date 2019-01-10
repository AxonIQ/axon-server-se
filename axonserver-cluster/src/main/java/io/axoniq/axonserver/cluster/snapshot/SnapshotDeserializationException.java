package io.axoniq.axonserver.cluster.snapshot;

/**
 * Exception that represents issues with deserialization of snapshot data.
 *
 * @author Milan Savic
 * @since 4.1
 */
public class SnapshotDeserializationException extends RuntimeException {

    /**
     * Constructs the SnapshotDeserializationException.
     *
     * @param message the detail message (which is saved for later retrieval by the {@link #getMessage()} method).
     * @param cause   the cause (which is saved for later retrieval by the {@link #getCause()} method).  (A
     *                <tt>null</tt> value is permitted, and indicates that the cause is nonexistent or unknown.)
     */
    public SnapshotDeserializationException(String message, Throwable cause) {
        super(message, cause);
    }
}
