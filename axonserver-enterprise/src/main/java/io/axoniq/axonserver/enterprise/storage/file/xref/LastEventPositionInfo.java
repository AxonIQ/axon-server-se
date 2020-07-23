package io.axoniq.axonserver.enterprise.storage.file.xref;

/**
 * @author Marc Gathier
 */
public class LastEventPositionInfo {

    private final long token;
    private final long lastSequence;

    public LastEventPositionInfo(long token, long lastSequence) {
        this.token = token;
        this.lastSequence = lastSequence;
    }


    public long token() {
        return token;
    }

    public long lastSequence() {
        return lastSequence;
    }
}
