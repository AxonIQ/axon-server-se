package io.axoniq.cli;

/**
 * Author: marc
 */
public class CommandExecutionException extends RuntimeException {

    private final int errorCode;
    private final String url;

    public CommandExecutionException(int errorCode, String url, String errorMessage) {
        super(errorMessage);
        this.errorCode = errorCode;
        this.url = url;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public String getUrl() {
        return url;
    }
}
