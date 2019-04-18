package io.axoniq.axonserver.cluster.replication.file;

/**
 * @author Marc Gathier
 */
public class ValidationResult {

    private final long segment;
    private final String message;
    private final long lastToken;
    private final boolean valid;

    public ValidationResult(long segment, long lastToken) {
        this.segment = segment;
        this.lastToken = lastToken;
        this.valid = true;
        this.message = null;
    }

    public ValidationResult(long segment, String message) {
        this.segment = segment;
        this.message = message;
        this.valid = false;
        this.lastToken = segment;
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

    public long getSegment() {
        return segment;
    }
}
