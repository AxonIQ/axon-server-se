package io.axoniq.axonhub.localstorage.file;

/**
 * Author: marc
 */
public class ValidationResult {
    private final long segment;
    private final long lastToken;
    private final boolean valid;
    private final String message;

    public ValidationResult(long segment, long lastToken) {
        this.segment = segment;
        this.lastToken = lastToken;
        this.valid = true;
        this.message = null;
    }
    public ValidationResult(long segment, String message) {
        this.segment = segment;
        this.lastToken = -1;
        this.valid = false;
        this.message = message;
    }

    public long getSegment() {
        return segment;
    }

    public long getLastToken() {
        return lastToken;
    }

    public boolean isValid() {
        return valid;
    }

    public String getMessage() {
        return message;
    }
}
