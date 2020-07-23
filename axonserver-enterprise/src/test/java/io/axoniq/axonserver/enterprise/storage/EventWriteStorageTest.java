package io.axoniq.axonserver.enterprise.storage;

import io.axoniq.axonserver.config.SystemInfoProvider;
import io.axoniq.axonserver.enterprise.storage.file.DatafileEventStoreFactory;
import io.axoniq.axonserver.enterprise.storage.file.DefaultMultiContextEventTransformerFactory;
import io.axoniq.axonserver.enterprise.storage.file.EmbeddedDBPropertiesProvider;
import io.axoniq.axonserver.enterprise.storage.multitier.MultiTierInformationProvider;
import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.localstorage.EventStorageEngine;
import io.axoniq.axonserver.localstorage.EventStoreFactory;
import io.axoniq.axonserver.localstorage.EventWriteStorage;
import io.axoniq.axonserver.localstorage.SerializedEvent;
import io.axoniq.axonserver.localstorage.file.EmbeddedDBProperties;
import io.axoniq.axonserver.localstorage.transaction.SingleInstanceTransactionManager;
import io.axoniq.axonserver.localstorage.transformation.DefaultEventTransformerFactory;
import io.axoniq.axonserver.metric.DefaultMetricCollector;
import io.axoniq.axonserver.metric.MeterFactory;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.*;
import org.junit.rules.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

import static org.mockito.Mockito.*;

/**
 * @author Marc Gathier
 */
public class EventWriteStorageTest {

    @ClassRule
    public static TemporaryFolder tempFolder = new TemporaryFolder();
    private EventWriteStorage testSubject;
    private EventStorageEngine datafileManagerChain;

    @Before
    public void setUp() {
        EmbeddedDBProperties embeddedDBProperties = new EmbeddedDBProperties(new SystemInfoProvider() {
        });
        embeddedDBProperties.getEvent().setStorage(
                tempFolder.getRoot().getAbsolutePath() + "/" + UUID.randomUUID().toString());
        embeddedDBProperties.getEvent().setSegmentSize(5120 * 1024L);
        embeddedDBProperties.getSnapshot().setStorage(tempFolder.getRoot().getAbsolutePath());

MeterFactory meterFactory = new MeterFactory(new SimpleMeterRegistry(), new DefaultMetricCollector());

        EventStoreFactory eventStoreFactory = new DatafileEventStoreFactory(new EmbeddedDBPropertiesProvider(
                embeddedDBProperties),
                                                                            new DefaultMultiContextEventTransformerFactory(
                                                                                    new DefaultEventTransformerFactory()),
                                                                            mock(MultiTierInformationProvider.class),
                                                                            null,
                                                                            meterFactory);
        datafileManagerChain = eventStoreFactory.createEventStorageEngine("default");
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
    public void addEmptyEventList() throws ExecutionException, InterruptedException {
        testSubject.store(Collections.emptyList()).get();
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
