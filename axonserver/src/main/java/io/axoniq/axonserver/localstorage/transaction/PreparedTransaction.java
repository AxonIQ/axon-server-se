package io.axoniq.axonserver.localstorage.transaction;

import io.axoniq.axonserver.localstorage.TransactionInformation;
import io.axoniq.axonserver.localstorage.transformation.ProcessedEvent;

import java.util.List;

/**
 * @author Marc Gathier
 */
public class PreparedTransaction {
    private final long token;
    private final List<ProcessedEvent> eventList;
    private final TransactionInformation transactionInformation;

    public PreparedTransaction(long token, List<ProcessedEvent> eventList, TransactionInformation transactionInformation) {
        this.token = token;
        this.eventList = eventList;
        this.transactionInformation = transactionInformation;
    }

    public long getToken() {
        return token;
    }

    public List<ProcessedEvent> getEventList() {
        return eventList;
    }

    public TransactionInformation getTransactionInformation() {
        return transactionInformation;
    }
}
