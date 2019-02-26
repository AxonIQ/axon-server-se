package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axonserver.localstorage.transaction.PreparedTransaction;
import io.axoniq.axonserver.localstorage.transformation.ProcessedEvent;

import java.util.List;

/**
 * @author Marc Gathier
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
