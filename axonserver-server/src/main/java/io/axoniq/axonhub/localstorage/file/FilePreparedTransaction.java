package io.axoniq.axonhub.localstorage.file;

import io.axoniq.axonhub.localstorage.transaction.PreparedTransaction;
import io.axoniq.axonhub.localstorage.transformation.ProcessedEvent;

import java.util.List;

/**
 * Author: marc
 */
public class FilePreparedTransaction extends PreparedTransaction {

    private final WritePosition writePosition;
    private final int eventSize;

    public FilePreparedTransaction(WritePosition writePosition, int eventSize, List<ProcessedEvent> eventList) {
        super( writePosition.sequence, eventList);
        this.writePosition = writePosition;
        this.eventSize = eventSize;
    }

    public WritePosition getWritePosition() {
        return writePosition;
    }

    public int getEventSize() {
        return eventSize;
    }
}
