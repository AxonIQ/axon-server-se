package io.axoniq.axonserver.enterprise.storage;

import io.axoniq.axonserver.enterprise.storage.file.DatafileEventStoreFactory;
import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.localstorage.EventStore;
import io.axoniq.axonserver.localstorage.EventStoreFactory;
import io.axoniq.axonserver.localstorage.EventWriteStorage;
import io.axoniq.axonserver.localstorage.SerializedEvent;
import io.axoniq.axonserver.localstorage.file.EmbeddedDBProperties;
import io.axoniq.axonserver.localstorage.transaction.DefaultStorageTransactionManagerFactory;
import io.axoniq.axonserver.localstorage.transaction.SingleInstanceTransactionManager;
import io.axoniq.axonserver.localstorage.transformation.DefaultEventTransformerFactory;
import org.junit.*;
import org.junit.rules.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

/**
 * Author: marc
 */
public class EventWriteStorageTest {

    @ClassRule
    public static TemporaryFolder tempFolder = new TemporaryFolder();
    private EventWriteStorage testSubject;
    private EventStore datafileManagerChain;

    @Before
    public void setUp() {
        EmbeddedDBProperties embeddedDBProperties = new EmbeddedDBProperties();
        embeddedDBProperties.getEvent().setStorage(tempFolder.getRoot().getAbsolutePath() + "/" + UUID.randomUUID().toString());
        embeddedDBProperties.getEvent().setSegmentSize(5120 * 1024L);
        embeddedDBProperties.getSnapshot().setStorage(tempFolder.getRoot().getAbsolutePath());
        EventStoreFactory eventStoreFactory = new DatafileEventStoreFactory(embeddedDBProperties, new DefaultEventTransformerFactory(),
                                                                            new DefaultStorageTransactionManagerFactory());
        datafileManagerChain = eventStoreFactory.createEventManagerChain("default");
        datafileManagerChain.init(false);
        testSubject = new EventWriteStorage(new SingleInstanceTransactionManager(datafileManagerChain));
    }

    @Test
    public void addEvent() throws ExecutionException, InterruptedException {
        Event event = Event.newBuilder().setAggregateIdentifier("1").setAggregateSequenceNumber(0).setAggregateType(
                "Demo").setPayload(SerializedObject.newBuilder().build()).build();
        testSubject.store(Collections.singletonList(new SerializedEvent(event))).get();
    }

    @Test
    public void addNonDomainEvent() throws ExecutionException, InterruptedException {
        Event event = Event.newBuilder().setMessageIdentifier("1").setPayload(SerializedObject.newBuilder().build()).build();
        testSubject.store(Collections.singletonList(new SerializedEvent(event))).get();
        event = Event.newBuilder().setMessageIdentifier("2").setPayload(SerializedObject.newBuilder().build()).build();
        testSubject.store(Collections.singletonList(new SerializedEvent(event))).get();
    }

    @Test(expected = ExecutionException.class)
    public void addEventWithInvalidSequenceNumber() throws ExecutionException, InterruptedException {
        Event event = Event.newBuilder().setAggregateIdentifier("2").setAggregateSequenceNumber(1).setAggregateType(
                "Demo").setPayload(SerializedObject.newBuilder().build()).build();
        testSubject.store(Collections.singletonList(new SerializedEvent(event))).get();
    }

    @Test(expected = ExecutionException.class)
    public void addEventWithInvalidSequenceNumber2() throws ExecutionException, InterruptedException {
        Event event = Event.newBuilder().setAggregateIdentifier("3").setAggregateSequenceNumber(0).setAggregateType(
                "Demo").setPayload(SerializedObject.newBuilder().build()).build();
        testSubject.store(Collections.singletonList(new SerializedEvent(event))).get();
        event = Event.newBuilder().setAggregateIdentifier("3").setAggregateSequenceNumber(2).setAggregateType("Demo")
                     .setPayload(SerializedObject.newBuilder().build()).build();
        testSubject.store(Collections.singletonList(new SerializedEvent(event))).get();
    }

    @Test
    public void addEventWithValidSequenceNumber() throws ExecutionException, InterruptedException {
        Event event = Event.newBuilder().setAggregateIdentifier("3").setAggregateSequenceNumber(0).setAggregateType(
                "Demo").setPayload(SerializedObject.newBuilder().build()).build();
        testSubject.store(Collections.singletonList(new SerializedEvent(event))).get();
        event = Event.newBuilder().setAggregateIdentifier("3").setAggregateSequenceNumber(1).setAggregateType("Demo")
                     .setPayload(SerializedObject.newBuilder().build()).build();
        testSubject.store(Collections.singletonList(new SerializedEvent(event))).get();
    }

    @Test
    public void addMultipleEvents() throws ExecutionException, InterruptedException {
        String aggId = UUID.randomUUID().toString();
        List<SerializedEvent> newEvents = new ArrayList<>();
        IntStream.range(0, 1000).forEach(i -> {
            newEvents.add(new SerializedEvent(Event.newBuilder().setAggregateIdentifier(aggId).setAggregateSequenceNumber(i)
                               .setAggregateType("Demo").setPayload(SerializedObject.newBuilder().build()).build()));
        });
        testSubject.store(newEvents).get();
    }

    @Test
    public void addEvenMoreEvents() {
        IntStream.range(0, 1000).parallel().forEach(j -> {
            String aggId = UUID.randomUUID().toString();
            List<SerializedEvent> newEvents = new ArrayList<>();
            IntStream.range(0, 100).forEach(i -> {
                newEvents.add(new SerializedEvent(Event.newBuilder().setAggregateIdentifier(aggId).setAggregateSequenceNumber(i)
                                   .setAggregateType("Demo").setPayload(SerializedObject.newBuilder().build()).build()));
            });
            try {
                testSubject.store(newEvents).get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        });
    }

}