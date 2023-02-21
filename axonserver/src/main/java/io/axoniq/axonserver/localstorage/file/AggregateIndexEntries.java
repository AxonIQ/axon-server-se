package io.axoniq.axonserver.localstorage.file;

/**
 * @author Stefan Dragisic
 */
public class AggregateIndexEntries {
    private final String aggregateId;
    private final IndexEntries entries;
    public AggregateIndexEntries(String aggregateId, IndexEntries entries) {
        this.aggregateId = aggregateId;
        this.entries = entries;
    }

    public String aggregateId() {
        return aggregateId;
    }

    public IndexEntries entries() {
        return entries;
    }
}
