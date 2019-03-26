package io.axoniq.axonserver.localstorage;

import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.EventWithToken;
import io.axoniq.axonserver.localstorage.transaction.PreparedTransaction;
import org.springframework.data.util.CloseableIterator;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Author: marc
 */
public class FakeEventStore implements EventStorageEngine {

    private final EventType eventType;

    public FakeEventStore(EventType eventType) {
        this.eventType = eventType;
    }

    @Override
    public void init(boolean validate) {

    }

    @Override
    public Optional<Long> getLastSequenceNumber(String aggregateIdentifier) {
        return Optional.empty();
    }

    @Override
    public Optional<SerializedEvent> getLastEvent(String aggregateId, long minSequenceNumber) {
        return Optional.empty();
    }

    @Override
    public void streamByAggregateId(String aggregateId, long actualMinSequenceNumber,
                                    Consumer<SerializedEvent> eventConsumer) {

    }

    @Override
    public void streamByAggregateId(String aggregateId, long actualMinSequenceNumber, long actualMaxSequenceNumber,
                                    int maxResults, Consumer<SerializedEvent> eventConsumer) {

    }

    @Override
    public PreparedTransaction prepareTransaction( List<SerializedEvent> eventList) {
        return null;
    }

    @Override
    public EventTypeContext getType() {
        return new EventTypeContext("FakeContext", eventType);
    }

    @Override
    public Iterator<SerializedTransactionWithToken> transactionIterator(long firstToken) {
        return null;
    }

    @Override
    public Iterator<SerializedTransactionWithToken> transactionIterator(long firstToken, long limitToken) {
        return null;
    }

    @Override
    public void query(long minToken, long minTimestamp, Predicate<EventWithToken> consumer) {

    }

    @Override
    public long getFirstToken() {
        return 0;
    }

    @Override
    public long getLastToken() {
        return 10000;
    }

    @Override
    public long nextToken() {
        return 10001;
    }

    @Override
    public long getTokenAt(long instant) {
        return 0;
    }

    @Override
    public CloseableIterator<SerializedEventWithToken> getGlobalIterator(long start) {
        return new CloseableIterator<SerializedEventWithToken>() {
            long sequence = start;
            int remaining = 100;
            @Override
            public void close() {

            }

            @Override
            public boolean hasNext() {
                return remaining > 0;
            }

            @Override
            public SerializedEventWithToken next() {
                remaining--;
                try {
                    SerializedEventWithToken serializedEventWithToken = new SerializedEventWithToken(sequence,
                                                                                                     Event.newBuilder()
                                                                                                          .setAggregateIdentifier(
                                                                                                                  "aaaa")
                                                                                                          .build());

                    sequence++;
                    return serializedEventWithToken;
                } catch (RuntimeException ex) {
                    ex.printStackTrace();
                    throw ex;
                }
            }
        };
    }
}
