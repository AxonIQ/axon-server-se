package io.axoniq.axonserver.enterprise.storage;

import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.enterprise.storage.file.DatafileEventStoreFactory;
import io.axoniq.axonserver.localstorage.EventStore;
import io.axoniq.axonserver.localstorage.EventStoreFactory;
import io.axoniq.axonserver.localstorage.EventWriteStorage;
import io.axoniq.axonserver.localstorage.file.EmbeddedDBProperties;
import io.axoniq.axonserver.localstorage.file.SegmentBasedEventStore;
import io.axoniq.axonserver.localstorage.transaction.DefaultStorageTransactionManagerFactory;
import io.axoniq.axonserver.localstorage.transaction.SingleInstanceTransactionManager;
import io.axoniq.axonserver.localstorage.transaction.StorageTransactionManager;
import io.axoniq.axonserver.localstorage.transformation.DefaultEventTransformerFactory;
import io.axoniq.axonserver.grpc.SerializedObject;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.stream.IntStream;

/**
 * Author: marc
 */
public class TestStorageContainer {

    private final EventStore datafileManagerChain;
    private final EventStore snapshotManagerChain;
    private EventWriteStorage eventWriter;

    public TestStorageContainer(File location) throws IOException {
        EmbeddedDBProperties embeddedDBProperties = new EmbeddedDBProperties();
        embeddedDBProperties.getEvent().setStorage(location.getAbsolutePath());
        embeddedDBProperties.getEvent().setSegmentSize(512*1024L);
        embeddedDBProperties.getEvent().setForceInterval(10000);
        embeddedDBProperties.getSnapshot().setStorage(location.getAbsolutePath());
        embeddedDBProperties.getSnapshot().setSegmentSize(512*1024L);
        EventStoreFactory eventStoreFactory = new DatafileEventStoreFactory(embeddedDBProperties, new DefaultEventTransformerFactory(),
                                                                            new DefaultStorageTransactionManagerFactory());
        datafileManagerChain = eventStoreFactory.createEventManagerChain("default");
        datafileManagerChain.init(false);
        snapshotManagerChain = eventStoreFactory.createSnapshotManagerChain("default");
        snapshotManagerChain.init(false);
        eventWriter = new EventWriteStorage(new SingleInstanceTransactionManager(datafileManagerChain));
    }


    public void createDummyEvents(int transactions, int transactionSize) {
        createDummyEvents(transactions, transactionSize, "");
    }
    public void createDummyEvents(int transactions, int transactionSize, String prefix) {
        CountDownLatch countDownLatch = new CountDownLatch(transactions);
        IntStream.range(0, transactions).parallel().forEach(j -> {
            String aggId = prefix + j;
            List<Event> newEvents = new ArrayList<>();
            IntStream.range(0, transactionSize).forEach(i -> {
                newEvents.add(Event.newBuilder().setAggregateIdentifier(aggId).setAggregateSequenceNumber(i).setAggregateType("Demo").setPayload(
                        SerializedObject
                                .newBuilder().build()).build());
            });
            eventWriter.store(newEvents).whenComplete((r,t)->countDownLatch.countDown());
        });
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public EventStore getDatafileManagerChain() {
        return datafileManagerChain;
    }

    public EventStore getSnapshotManagerChain() {
        return snapshotManagerChain;
    }

    public EventWriteStorage getEventWriter() {
        return eventWriter;
    }

    public StorageTransactionManager getTransactionManager(EventStore datafileManagerChain) {
        return new SingleInstanceTransactionManager(datafileManagerChain);
    }

    public SegmentBasedEventStore getPrimary() {
        return (SegmentBasedEventStore)datafileManagerChain;
    }

    public void close() {
        datafileManagerChain.cleanup();
        snapshotManagerChain.cleanup();
    }
}
