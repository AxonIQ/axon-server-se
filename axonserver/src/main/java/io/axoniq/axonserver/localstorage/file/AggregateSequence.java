package io.axoniq.axonserver.localstorage.file;

/**
 * @author Stefan Dragisic
 */
public class AggregateSequence {

    private final String aggregateId;
    private final long lastSequenceNumber;

    public AggregateSequence( String aggregateId, long lastSequenceNumber) {
        this.aggregateId= aggregateId;
        this.lastSequenceNumber = lastSequenceNumber;
    }

    public String aggregateId() {
        return aggregateId;
    }

    public long lastSequenceNumber() {
        return lastSequenceNumber;
    }
}
