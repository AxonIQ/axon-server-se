package io.axoniq.axonserver.localstorage;

import io.axoniq.axondb.Event;
import io.axoniq.axonserver.localstorage.file.DatafileEventStoreFactory;
import io.axoniq.axonserver.localstorage.file.EmbeddedDBProperties;
import io.axoniq.axonserver.localstorage.transaction.DefaultStorageTransactionManagerFactory;
import io.axoniq.axonserver.localstorage.transaction.SingleInstanceTransactionManager;
import io.axoniq.axonserver.localstorage.transformation.DefaultEventTransformerFactory;
import io.axoniq.platform.SerializedObject;
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

    @After
    public void tearDown() {
        datafileManagerChain.cleanup();
    }

    @Test
    public void addEvent() throws ExecutionException, InterruptedException {
        Event event = Event.newBuilder().setAggregateIdentifier("1").setAggregateSequenceNumber(0).setAggregateType(
                "Demo").setPayload(SerializedObject.newBuilder().build()).build();
        testSubject.store(Collections.singletonList(event)).get();
    }

    @Test
    public void addNonDomainEvent() throws ExecutionException, InterruptedException {
        Event event = Event.newBuilder().setMessageIdentifier("1").setPayload(SerializedObject.newBuilder().build()).build();
        testSubject.store(Collections.singletonList(event)).get();
        event = Event.newBuilder().setMessageIdentifier("2").setPayload(SerializedObject.newBuilder().build()).build();
        testSubject.store(Collections.singletonList(event)).get();
    }

    @Test(expected = ExecutionException.class)
    public void addEventWithInvalidSequenceNumber() throws ExecutionException, InterruptedException {
        Event event = Event.newBuilder().setAggregateIdentifier("2").setAggregateSequenceNumber(1).setAggregateType(
                "Demo").setPayload(SerializedObject.newBuilder().build()).build();
        testSubject.store(Collections.singletonList(event)).get();
    }

    @Test(expected = ExecutionException.class)
    public void addEventWithInvalidSequenceNumber2() throws ExecutionException, InterruptedException {
        Event event = Event.newBuilder().setAggregateIdentifier("3").setAggregateSequenceNumber(0).setAggregateType(
                "Demo").setPayload(SerializedObject.newBuilder().build()).build();
        testSubject.store(Collections.singletonList(event)).get();
        event = Event.newBuilder().setAggregateIdentifier("3").setAggregateSequenceNumber(2).setAggregateType("Demo")
                     .setPayload(SerializedObject.newBuilder().build()).build();
        testSubject.store(Collections.singletonList(event)).get();
    }

    @Test
    public void addEventWithValidSequenceNumber() throws ExecutionException, InterruptedException {
        Event event = Event.newBuilder().setAggregateIdentifier("3").setAggregateSequenceNumber(0).setAggregateType(
                "Demo").setPayload(SerializedObject.newBuilder().build()).build();
        testSubject.store(Collections.singletonList(event)).get();
        event = Event.newBuilder().setAggregateIdentifier("3").setAggregateSequenceNumber(1).setAggregateType("Demo")
                     .setPayload(SerializedObject.newBuilder().build()).build();
        testSubject.store(Collections.singletonList(event)).get();
    }

    @Test
    public void addMultipleEvents() throws ExecutionException, InterruptedException {
        String aggId = UUID.randomUUID().toString();
        List<Event> newEvents = new ArrayList<>();
        IntStream.range(0, 1000).forEach(i -> {
            newEvents.add(Event.newBuilder().setAggregateIdentifier(aggId).setAggregateSequenceNumber(i)
                               .setAggregateType("Demo").setPayload(SerializedObject.newBuilder().build()).build());
        });
        testSubject.store(newEvents).get();
    }

    @Test
    public void addEvenMoreEvents() {
        IntStream.range(0, 1000).parallel().forEach(j -> {
            String aggId = UUID.randomUUID().toString();
            List<Event> newEvents = new ArrayList<>();
            IntStream.range(0, 100).forEach(i -> {
                newEvents.add(Event.newBuilder().setAggregateIdentifier(aggId).setAggregateSequenceNumber(i)
                                   .setAggregateType("Demo").setPayload(SerializedObject.newBuilder().build()).build());
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