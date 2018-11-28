package io.axoniq.axonserver.cluster.exception;

/**
 * Author: marc
 */
public class LogException extends RuntimeException {


    private final ErrorCode errorCode;

    public LogException(ErrorCode errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public LogException(ErrorCode errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }
}
