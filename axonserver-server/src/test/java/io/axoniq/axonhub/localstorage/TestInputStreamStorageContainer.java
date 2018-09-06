package io.axoniq.axonhub.localstorage;

import io.axoniq.axondb.Event;
import io.axoniq.axonhub.localstorage.file.EmbeddedDBProperties;
import io.axoniq.axonhub.localstorage.file.LowMemoryEventStoreFactory;
import io.axoniq.axonhub.localstorage.file.SegmentBasedEventStore;
import io.axoniq.axonhub.localstorage.transaction.DefaultStorageTransactionManagerFactory;
import io.axoniq.axonhub.localstorage.transaction.SingleInstanceTransactionManager;
import io.axoniq.axonhub.localstorage.transaction.StorageTransactionManager;
import io.axoniq.axonhub.localstorage.transformation.DefaultEventTransformerFactory;
import io.axoniq.platform.SerializedObject;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;

/**
 * Author: marc
 */
public class TestInputStreamStorageContainer {

    private final EventStore datafileManagerChain;
    private final EventStore snapshotManagerChain;
    private EventWriteStorage eventWriter;

    public TestInputStreamStorageContainer(File location) throws IOException {
        EmbeddedDBProperties embeddedDBProperties = new EmbeddedDBProperties();
        embeddedDBProperties.getEvent().setStorage(location.getAbsolutePath());
        embeddedDBProperties.getEvent().setSegmentSize(256*1024L);
        embeddedDBProperties.getEvent().setForceInterval(10000);
        embeddedDBProperties.getSnapshot().setStorage(location.getAbsolutePath());
        EventStoreFactory eventStoreFactory = new LowMemoryEventStoreFactory(embeddedDBProperties, new DefaultEventTransformerFactory(),
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
        IntStream.range(0, transactions).parallel().forEach(j -> {
            String aggId = prefix + j;
            List<Event> newEvents = new ArrayList<>();
            IntStream.range(0, transactionSize).forEach(i -> {
                newEvents.add(Event.newBuilder().setAggregateIdentifier(aggId).setAggregateSequenceNumber(i).setAggregateType("Demo").setPayload(
                        SerializedObject
                                .newBuilder().build()).build());
            });
            try {
                eventWriter.store(newEvents).get(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (ExecutionException | TimeoutException ignored) {
                // Ignore during test
                ignored.printStackTrace();
            }
        });
        try {
            Thread.sleep(500);
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

}
