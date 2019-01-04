package io.axoniq.axonserver.localstorage.transaction;

import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.localstorage.EventStore;
import io.axoniq.axonserver.localstorage.SerializedEvent;
import io.axoniq.axonserver.localstorage.StorageCallback;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Author: marc
 */
public class SingleInstanceTransactionManager implements StorageTransactionManager{
    private final EventStore datafileManagerChain;

    public SingleInstanceTransactionManager(
            EventStore datafileManagerChain) {
        this.datafileManagerChain = datafileManagerChain;
    }

    @Override
    public CompletableFuture<Long> store(List<SerializedEvent> eventList) {
        return datafileManagerChain.store(datafileManagerChain.prepareTransaction(eventList));
    }

    @Override
    public long getLastToken() {
        return datafileManagerChain.getLastToken();
    }

    @Override
    public void reserveSequenceNumbers(List<SerializedEvent> eventList) {
        datafileManagerChain.reserveSequenceNumbers(eventList);
    }
}
