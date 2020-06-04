package io.axoniq.axonserver.cluster.exception;

/**
 * Thrown in cases there is a bug in RAFT protocol implementation.
 *
 * @author Milan Savic
 * @since 4.3.5
 */
public class RaftException extends RuntimeException {

    private final ErrorCode errorCode;

    public RaftException(ErrorCode errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }
}
