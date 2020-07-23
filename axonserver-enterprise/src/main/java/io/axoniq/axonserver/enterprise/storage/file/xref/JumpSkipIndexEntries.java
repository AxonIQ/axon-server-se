package io.axoniq.axonserver.enterprise.storage.file.xref;

import io.axoniq.axonserver.localstorage.file.IndexEntry;
import io.axoniq.axonserver.localstorage.file.StandardIndexEntries;

import java.util.List;

/**
 * Extension of {@link StandardIndexEntries} which keeps track of information specific for
 * the {@link JumpSkipIndexManager}. While adding entries to an active index it keeps the highest sequence and token
 * for an aggregate.
 * Stores and reads the token of the last event before the current segment.
 *
 * @author Marc Gathier
 * @since 4.4
 */
public class JumpSkipIndexEntries extends StandardIndexEntries {

    private final long previousToken;
    private long lastSequenceNumber;
    private long lastToken;

    /**
     * @param previousToken       the token of the last event for the aggregate before the current segment
     * @param firstSequenceNumber the sequence number of the first event of the aggregate in the current segment
     * @param entries             list of positions of the events in the current segment
     */
    public JumpSkipIndexEntries(long previousToken, long firstSequenceNumber,
                                List<Integer> entries) {
        super(firstSequenceNumber, entries);
        this.previousToken = previousToken;
    }

    /**
     * Returns the token and sequence number of the last event.
     *
     * @return the token and sequence number of the last event
     */
    public LastEventPositionInfo lastEventPositionInfo() {
        if (entries.isEmpty()) {
            return null;
        }
        return new LastEventPositionInfo(lastToken, lastSequenceNumber);
    }

    /**
     * Adds an index entry to the list of entries.
     *
     * @param indexEntry the entry to add.
     */
    @Override
    public void add(IndexEntry indexEntry) {
        super.add(indexEntry);
        lastSequenceNumber = indexEntry.getSequenceNumber();
        lastToken = indexEntry.getToken();
    }

    @Override
    public void addAll(List<IndexEntry> indexEntries) {
        super.addAll(indexEntries);
        IndexEntry indexEntry = indexEntries.get(indexEntries.size() - 1);
        lastSequenceNumber = indexEntry.getSequenceNumber();
        lastToken = indexEntry.getToken();
    }

    /**
     * Returns the token of the last event before the current segment.
     *
     * @return previous token
     */
    public long previousToken() {
        return previousToken;
    }
}
