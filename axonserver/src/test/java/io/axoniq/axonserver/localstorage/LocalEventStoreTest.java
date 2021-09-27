/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage;

import io.axoniq.axonserver.plugin.ExecutionContext;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.EventWithToken;
import io.axoniq.axonserver.grpc.event.GetAggregateEventsRequest;
import io.axoniq.axonserver.grpc.event.GetAggregateSnapshotsRequest;
import io.axoniq.axonserver.grpc.event.GetEventsRequest;
import io.axoniq.axonserver.grpc.event.QueryEventsRequest;
import io.axoniq.axonserver.interceptor.EventInterceptors;
import io.axoniq.axonserver.localstorage.transaction.StorageTransactionManager;
import io.axoniq.axonserver.localstorage.transaction.StorageTransactionManagerFactory;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.*;
import org.springframework.data.util.CloseableIterator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.IntStream;

import static io.axoniq.axonserver.test.AssertUtils.assertWithin;
import static junit.framework.TestCase.assertEquals;

/**
 * @author Marc Gathier
 */
public class LocalEventStoreTest {

    private final CountingEventInterceptors eventInterceptors = new CountingEventInterceptors();
    private LocalEventStore testSubject;

    @Before
    public void setUp() throws Exception {
        Event[] events = {
                Event.newBuilder().setAggregateIdentifier("123").setAggregateSequenceNumber(0)
                     .setTimestamp(System.currentTimeMillis())
                        .build(),
                Event.newBuilder().setAggregateIdentifier("123").setAggregateSequenceNumber(1).build(),
                Event.newBuilder().setAggregateIdentifier("123").setAggregateSequenceNumber(2).build(),
                Event.newBuilder().setAggregateIdentifier("123").setAggregateSequenceNumber(3).build(),
                Event.newBuilder().setAggregateIdentifier("123").setAggregateSequenceNumber(4).build(),
                Event.newBuilder().setAggregateIdentifier("123").setAggregateSequenceNumber(5).build(),
                Event.newBuilder().setAggregateIdentifier("123").setAggregateSequenceNumber(6).build(),
                Event.newBuilder().setAggregateIdentifier("123").setAggregateSequenceNumber(7).build(),
        };

        Event[] snapshots = {
                Event.newBuilder().setAggregateIdentifier("123").setAggregateSequenceNumber(1).build(),
                Event.newBuilder().setAggregateIdentifier("123").setAggregateSequenceNumber(2).build(),
                Event.newBuilder().setAggregateIdentifier("123").setAggregateSequenceNumber(4).build(),
                Event.newBuilder().setAggregateIdentifier("123").setAggregateSequenceNumber(6).build(),
        };
        EventStoreFactory eventStoreFactory = new EventStoreFactory() {
            @Override
            public EventStorageEngine createEventStorageEngine(String context) {
                return new TestEventStore(EventType.EVENT, events);
            }

            @Override
            public EventStorageEngine createSnapshotStorageEngine(String context) {
                return new TestEventStore(EventType.SNAPSHOT, snapshots);
            }
        };

        StorageTransactionManagerFactory transactionManagerFactory = eventStore -> new StorageTransactionManager() {
            @Override
            public CompletableFuture<Long> store(List<Event> eventList) {
                return eventStore.store(eventList);
            }

            @Override
            public Runnable reserveSequenceNumbers(List<Event> eventList) {
                return () -> {
                };
            }

        };

        testSubject = new LocalEventStore(eventStoreFactory,
                                          new SimpleMeterRegistry(),
                                          transactionManagerFactory,
                                          eventInterceptors);
        testSubject.initContext("demo", false);
        testSubject.start();
    }

    @Test
    public void appendSnapshot() throws ExecutionException, InterruptedException {
        StepVerifier.create(testSubject.appendSnapshot("demo", Event.newBuilder().build(), null))
                    .verifyComplete();
        assertEquals(1, eventInterceptors.appendSnapshot);
        assertEquals(1, eventInterceptors.snapshotPostCommit);
    }

    @Test
    public void appendSnapshotCompensate() throws InterruptedException {
        StepVerifier.create(testSubject.appendSnapshot("demo",
                                                       Event.newBuilder().setMessageIdentifier("FAIL").build(),
                                                       null))
                    .verifyError();
        assertEquals(1, eventInterceptors.appendSnapshot);
        assertEquals(0, eventInterceptors.snapshotPostCommit);
        assertEquals(1, eventInterceptors.compensations.size());
        assertEquals("Compensate Append Snapshot", eventInterceptors.compensations.get(0));
    }

    @Test
    public void createAppendEventConnection() throws ExecutionException, InterruptedException {
        Flux<SerializedEvent> events = Flux.fromStream(IntStream.range(0, 3)
                                                      .mapToObj(i -> new SerializedEvent(Event.getDefaultInstance())));
        StepVerifier.create(testSubject.appendEvents("demo", events, null))
                    .verifyComplete();

        assertEquals(3, eventInterceptors.appendEvent);
        assertEquals(1, eventInterceptors.eventsPreCommit);
        assertEquals(1, eventInterceptors.eventsPostCommit);
    }

    @Test
    public void createAppendEventConnectionCompensateAppendEntries() throws InterruptedException {
        eventInterceptors.failPreCommit = true;
        Flux<SerializedEvent> events = Flux.fromStream(IntStream.range(0, 2)
                                                      .mapToObj(i -> new SerializedEvent(Event.newBuilder()
                                                                          .setMessageIdentifier("FAIL")
                                                                          .build())));
        StepVerifier.create(testSubject.appendEvents("demo", events, null))
                    .verifyError();
        assertEquals(2, eventInterceptors.appendEvent);
        assertEquals(0, eventInterceptors.eventsPreCommit);
        assertEquals(2, eventInterceptors.compensations.size());
        assertEquals("Compensate Append Event", eventInterceptors.compensations.get(0));
        assertEquals("Compensate Append Event", eventInterceptors.compensations.get(1));
    }

    @Test
    public void createAppendEventConnectionCompensatePreCommitAndAppendEntry() throws InterruptedException {
        Flux<SerializedEvent> events = Flux.just(new SerializedEvent(Event.newBuilder().setMessageIdentifier("FAIL").build()));
        StepVerifier.create(testSubject.appendEvents("demo", events, null))
                    .verifyError();
        assertEquals(1, eventInterceptors.eventsPreCommit);
        assertEquals(2, eventInterceptors.compensations.size());
        assertEquals("Compensate Events Pre Commit", eventInterceptors.compensations.get(0));
        assertEquals("Compensate Append Event", eventInterceptors.compensations.get(1));
    }

    @Test
    public void aggregateEvents() {
        Flux<SerializedEvent> events = testSubject.aggregateEvents("demo", null,
                                                                 GetAggregateEventsRequest.newBuilder()
                                                                                          .setAllowSnapshots(true)
                                                                                          .build());

        StepVerifier.create(events.map(SerializedEvent::asEvent)
                                  .map(Event::getAggregateSequenceNumber))
                    .expectNext(6L, 7L)
                    .verifyComplete();
        assertEquals(1, eventInterceptors.readSnapshot);
        assertEquals(1, eventInterceptors.readEvent);
    }

    @Test
    public void aggregateEventsNoSnapshots() {
        Flux<SerializedEvent> events = testSubject.aggregateEvents("demo", null,
                                                                 GetAggregateEventsRequest.newBuilder()
                                                                                          .build());

        StepVerifier.create(events.map(SerializedEvent::asEvent)
                                  .map(Event::getAggregateSequenceNumber))
                    .expectNext(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L)
                    .verifyComplete();

        assertEquals(0, eventInterceptors.readSnapshot);
        assertEquals(8, eventInterceptors.readEvent);
    }

    @Test
    public void aggregateSnapshots() {
        StepVerifier.create(testSubject.aggregateSnapshots("demo",
                                       null,
                                       GetAggregateSnapshotsRequest.getDefaultInstance()))
                    .expectNextCount(4)
                    .verifyComplete();

        assertEquals(4, eventInterceptors.readSnapshot);
        assertEquals(0, eventInterceptors.readEvent);
    }

    @Test
    public void events() throws InterruptedException {
        GetEventsRequest request = GetEventsRequest.newBuilder()
                                                   .setNumberOfPermits(100)
                                                   .build();
        Sinks.Many<GetEventsRequest> manyReq = Sinks.many().unicast().onBackpressureBuffer();

        testSubject.events("demo", null, manyReq.asFlux())
                   .subscribe();

        assertEquals(Sinks.EmitResult.OK, manyReq.tryEmitNext(request));
        assertWithin(1, TimeUnit.SECONDS, () -> assertEquals(8, eventInterceptors.readEvent));
        assertEquals(Sinks.EmitResult.OK, manyReq.tryEmitComplete());
    }

    @Test
    public void queryEvents() throws InterruptedException {
        AtomicReference<FluxSink<QueryEventsRequest>> sinkRef = new AtomicReference<>();
        testSubject.queryEvents("demo",
                                Flux.create(sinkRef::set),
                                null)
                   .subscribe();
        sinkRef.get()
               .next(QueryEventsRequest.newBuilder()
                                       .setNumberOfPermits(100L)
                                       .setQuery("limit(100)")
                                       .build());

        assertWithin(1, TimeUnit.SECONDS, () -> assertEquals(8, eventInterceptors.readEvent));
        sinkRef.get()
               .complete();
    }

    private static class CountingEventInterceptors implements EventInterceptors {

        boolean failAppend;
        boolean failPreCommit;
        int appendEvent;
        int appendSnapshot;
        int eventsPreCommit;
        int eventsPostCommit;
        int snapshotPostCommit;
        int readEvent;
        int readSnapshot;
        List<String> compensations = new ArrayList<>();

        @Override
        public Event appendEvent(Event event, ExecutionContext executionContext) {
            if (failAppend) {
                throw new RuntimeException("appendEvent");
            }
            executionContext.onFailure((t, e) -> compensations.add("Compensate Append Event"));
            appendEvent++;
            return event;
        }

        @Override
        public Event appendSnapshot(Event snapshot, ExecutionContext executionContext) {
            if (failAppend) {
                throw new RuntimeException("appendSnapshot");
            }
            executionContext.onFailure((t, e) -> compensations.add("Compensate Append Snapshot"));
            appendSnapshot++;
            return snapshot;
        }

        @Override
        public void eventsPreCommit(List<Event> events, ExecutionContext executionContext) {
            if (failPreCommit) {
                throw new RuntimeException("eventsPreCommit");
            }
            executionContext.onFailure((t, e) -> compensations.add("Compensate Events Pre Commit"));
            eventsPreCommit++;
        }

        @Override
        public void eventsPostCommit(List<Event> events, ExecutionContext executionContext) {
            eventsPostCommit++;
        }

        @Override
        public void snapshotPostCommit(Event snapshot, ExecutionContext executionContext) {
            snapshotPostCommit++;
        }

        @Override
        public Event readSnapshot(Event snapshot, ExecutionContext executionContext) {
            readSnapshot++;
            return snapshot;
        }

        @Override
        public Event readEvent(Event event, ExecutionContext executionContext) {
            readEvent++;
            return event;
        }

        @Override
        public boolean noReadInterceptors(String context) {
            return false;
        }

        @Override
        public boolean noEventReadInterceptors(String context) {
            return false;
        }

        @Override
        public boolean noSnapshotReadInterceptors(String context) {
            return false;
        }
    }

    private static class TestEventStore extends FakeEventStore {

        private final Event[] events;

        public TestEventStore(EventType eventType, Event[] events) {
            super(eventType);
            this.events = events;
        }

        @Override
        public void processEventsPerAggregate(String aggregateId, long actualMinSequenceNumber,
                                              long actualMaxSequenceNumber, long minToken,
                                              Consumer<SerializedEvent> eventConsumer) {
            Arrays.stream(events).filter(e -> e.getAggregateSequenceNumber() >= actualMinSequenceNumber)
                  .forEach(e -> eventConsumer.accept(new SerializedEvent(e)));
        }

        @Override
        public Flux<SerializedEvent> eventsPerAggregate(String aggregateId, long firstSequenceNumber,
                                                        long lastSequenceNumber, long minToken) {
            return Flux.fromStream(Arrays.stream(events)
                                         .filter(e -> e.getAggregateSequenceNumber() >= firstSequenceNumber))
                       .map(SerializedEvent::new);
        }

        @Override
        public CloseableIterator<SerializedEventWithToken> getGlobalIterator(long start) {
            return new CloseableIterator<SerializedEventWithToken>() {
                int index = 0;

                @Override
                public boolean hasNext() {
                    return index < events.length;
                }

                @Override
                public SerializedEventWithToken next() {
                    SerializedEventWithToken serializedEventWithToken = new SerializedEventWithToken(index,
                                                                                                     events[index]);
                    index++;
                    return serializedEventWithToken;
                }

                @Override
                public void close() {

                }
            };
        }

        @Override
        public Optional<Long> getLastSequenceNumber(String aggregateIdentifier, SearchHint... hints) {
            return Optional.of(7L);
        }

        @Override
        public void query(QueryOptions queryOptions, Predicate<EventWithToken> consumer) {
            for (int i = 0; i < events.length; i++) {
                if (!consumer.test(EventWithToken.newBuilder()
                                                 .setToken(i).setEvent(events[i]).build())) {
                    return;
                }
            }
        }

        @Override
        public CompletableFuture<Long> store(List<Event> eventList) {
            if (eventList.get(0).getMessageIdentifier().equals("FAIL")) {
                CompletableFuture<Long> completableFuture = new CompletableFuture<>();
                completableFuture.completeExceptionally(new RuntimeException("Empty transaction"));
                return completableFuture;
            }
            return CompletableFuture.completedFuture(0L);
        }

        @Override
        public void processEventsPerAggregateHighestFirst(String aggregateId, long actualMinSequenceNumber,
                                                          long actualMaxSequenceNumber, int maxResults,
                                                          Consumer<SerializedEvent> eventConsumer) {
            for (int i = events.length - 1; i >= 0; i--) {
                eventConsumer.accept(new SerializedEvent(events[i]));
            }
        }

        @Override
        public Optional<SerializedEvent> getLastEvent(String aggregateId, long minSequenceNumber,
                                                      long maxSequenceNumber) {
            return Optional.of(new SerializedEvent(events[events.length - 1]));
        }
    }
}