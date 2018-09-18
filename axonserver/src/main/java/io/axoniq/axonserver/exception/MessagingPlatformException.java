package io.axoniq.axonserver.exception;

/**
 * Author: marc
 */
public class MessagingPlatformException extends RuntimeException {
    private final ErrorCode errorCode;
    public MessagingPlatformException(ErrorCode errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public MessagingPlatformException(ErrorCode errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }

    @Override
    public String getMessage() {
        return "[" + errorCode.getCode() + "] " + super.getMessage();
    }
}
