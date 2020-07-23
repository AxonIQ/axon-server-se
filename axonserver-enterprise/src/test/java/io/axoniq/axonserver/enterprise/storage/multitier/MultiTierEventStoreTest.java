package io.axoniq.axonserver.enterprise.storage.multitier;

import io.axoniq.axonserver.enterprise.cluster.CompletableStreamObserver;
import io.axoniq.axonserver.enterprise.cluster.manager.LeaderEventStoreLocator;
import io.axoniq.axonserver.enterprise.messaging.event.LowerTierEventStore;
import io.axoniq.axonserver.grpc.event.ColumnsResponse;
import io.axoniq.axonserver.grpc.event.Confirmation;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.GetAggregateEventsRequest;
import io.axoniq.axonserver.grpc.event.GetAggregateSnapshotsRequest;
import io.axoniq.axonserver.grpc.event.GetEventsRequest;
import io.axoniq.axonserver.grpc.event.GetFirstTokenRequest;
import io.axoniq.axonserver.grpc.event.GetLastTokenRequest;
import io.axoniq.axonserver.grpc.event.GetTokenAtRequest;
import io.axoniq.axonserver.grpc.event.QueryEventsRequest;
import io.axoniq.axonserver.grpc.event.QueryEventsResponse;
import io.axoniq.axonserver.grpc.event.ReadHighestSequenceNrRequest;
import io.axoniq.axonserver.grpc.event.ReadHighestSequenceNrResponse;
import io.axoniq.axonserver.grpc.event.TrackingToken;
import io.axoniq.axonserver.grpc.internal.TransactionWithToken;
import io.axoniq.axonserver.localstorage.SerializedEvent;
import io.axoniq.axonserver.message.event.EventStore;
import io.axoniq.axonserver.test.FakeStreamObserver;
import io.grpc.stub.StreamObserver;
import org.junit.*;
import org.mockito.stubbing.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.LongStream;

import static io.axoniq.axonserver.test.AssertUtils.assertWithin;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author Marc Gathier
 */
public class MultiTierEventStoreTest {

    private static final String CONTEXT = "Context";
    private static final Logger logger = LoggerFactory.getLogger(MultiTierEventStoreTest.class);
    private static final String AGGREGATE_PARTLY_PRIMARY = "AGGREGATE1";
    private static final String AGGREGATE_FULLY_PRIMARY = "AGGREGATE2";
    private static final String AGGREGATE_ONLY_SECONDARY = "AGGREGATE3";
    private static final String AGGREGATE_PARTLY_PRIMARY_SNAPSHOT_PRIMARY = "AGGREGATE4";
    private static final String AGGREGATE_PARTLY_PRIMARY_SNAPSHOT_SECONDARY = "AGGREGATE5";
    private static final String AGGREGATE_PARTLY_PRIMARY_MORE_RECENT_SNAPSHOT_SECONDARY = "AGGREGATE6";
    public static final String LEADER = "LEADER";
    public static final String LOCAL = "LOCAL";
    public static final String SECONDARY = "SECONDARY";
    private MultiTierEventStore testSubject;

    @Before
    public void setup() {
        LeaderEventStoreLocator leaderEventStoreLocator = mock(LeaderEventStoreLocator.class);
        MockEventStore leaderEventStore = new MockEventStore(100, 200, LEADER)
                .withFirstAggregateSequenceNumber(AGGREGATE_PARTLY_PRIMARY, 5)
                .withLastAggregateSequenceNumber(AGGREGATE_PARTLY_PRIMARY, 10)
                .withFirstAggregateSequenceNumber(AGGREGATE_PARTLY_PRIMARY_SNAPSHOT_PRIMARY, 5)
                .withLastAggregateSequenceNumber(AGGREGATE_PARTLY_PRIMARY_SNAPSHOT_PRIMARY, 10)
                .withSnapshot(AGGREGATE_PARTLY_PRIMARY_SNAPSHOT_PRIMARY, 7)
                .withFirstAggregateSequenceNumber(AGGREGATE_PARTLY_PRIMARY_SNAPSHOT_SECONDARY, 5)
                .withLastAggregateSequenceNumber(AGGREGATE_PARTLY_PRIMARY_SNAPSHOT_SECONDARY, 10)
                .withFirstAggregateSequenceNumber(AGGREGATE_FULLY_PRIMARY, 0)
                .withLastAggregateSequenceNumber(AGGREGATE_FULLY_PRIMARY, 10)
                .withFirstAggregateSequenceNumber(AGGREGATE_PARTLY_PRIMARY_MORE_RECENT_SNAPSHOT_SECONDARY, 5)
                .withLastAggregateSequenceNumber(AGGREGATE_PARTLY_PRIMARY_MORE_RECENT_SNAPSHOT_SECONDARY, 10);
        MockEventStore localEventStore = new MockEventStore(100, 199, LOCAL)
                .withFirstAggregateSequenceNumber(AGGREGATE_PARTLY_PRIMARY, 5)
                .withLastAggregateSequenceNumber(AGGREGATE_PARTLY_PRIMARY, 10)
                .withFirstAggregateSequenceNumber(AGGREGATE_PARTLY_PRIMARY_SNAPSHOT_PRIMARY, 5)
                .withLastAggregateSequenceNumber(AGGREGATE_PARTLY_PRIMARY_SNAPSHOT_PRIMARY, 10)
                .withSnapshot(AGGREGATE_PARTLY_PRIMARY_SNAPSHOT_PRIMARY, 7)
                .withFirstAggregateSequenceNumber(AGGREGATE_PARTLY_PRIMARY_SNAPSHOT_SECONDARY, 5)
                .withLastAggregateSequenceNumber(AGGREGATE_PARTLY_PRIMARY_SNAPSHOT_SECONDARY, 10)
                .withFirstAggregateSequenceNumber(AGGREGATE_FULLY_PRIMARY, 0)
                .withLastAggregateSequenceNumber(AGGREGATE_FULLY_PRIMARY, 10);
        MockEventStore secondaryEventStore = new MockLowerTierEventStore(0, 190, SECONDARY)
                .withFirstAggregateSequenceNumber(AGGREGATE_PARTLY_PRIMARY, 0)
                .withLastAggregateSequenceNumber(AGGREGATE_PARTLY_PRIMARY, 10)
                .withFirstAggregateSequenceNumber(AGGREGATE_PARTLY_PRIMARY_SNAPSHOT_PRIMARY, 0)
                .withLastAggregateSequenceNumber(AGGREGATE_PARTLY_PRIMARY_SNAPSHOT_PRIMARY, 10)
                .withSnapshot(AGGREGATE_PARTLY_PRIMARY_SNAPSHOT_PRIMARY, 7)
                .withFirstAggregateSequenceNumber(AGGREGATE_PARTLY_PRIMARY_SNAPSHOT_SECONDARY, 0)
                .withLastAggregateSequenceNumber(AGGREGATE_PARTLY_PRIMARY_SNAPSHOT_SECONDARY, 10)
                .withSnapshot(AGGREGATE_PARTLY_PRIMARY_SNAPSHOT_SECONDARY, 3)
                .withFirstAggregateSequenceNumber(AGGREGATE_PARTLY_PRIMARY_MORE_RECENT_SNAPSHOT_SECONDARY, 0)
                .withLastAggregateSequenceNumber(AGGREGATE_PARTLY_PRIMARY_MORE_RECENT_SNAPSHOT_SECONDARY, 10)
                .withSnapshot(AGGREGATE_PARTLY_PRIMARY_MORE_RECENT_SNAPSHOT_SECONDARY, 8)
                .withFirstAggregateSequenceNumber(AGGREGATE_FULLY_PRIMARY, 0)
                .withLastAggregateSequenceNumber(AGGREGATE_FULLY_PRIMARY, 10)
                .withFirstAggregateSequenceNumber(AGGREGATE_ONLY_SECONDARY, 0)
                .withLastAggregateSequenceNumber(AGGREGATE_ONLY_SECONDARY, 10);
        when(leaderEventStoreLocator.getEventStore(CONTEXT)).thenReturn(leaderEventStore);
        MultiTierReaderEventStoreLocator multiTierReaderEventStoreLocator =
                mock(MultiTierReaderEventStoreLocator.class);

        LowerTierEventStoreLocator lowerTierEventStoreLocator = mock(LowerTierEventStoreLocator.class);
        when(lowerTierEventStoreLocator.getEventStore(CONTEXT)).thenReturn((LowerTierEventStore) secondaryEventStore);

        when(multiTierReaderEventStoreLocator.getEventStoreWithToken(anyString(), anyBoolean(), anyLong()))
                .then((Answer<EventStore>) invocation -> {
                    boolean forceReadFromLeader = invocation.getArgument(1);
                    long token = invocation.getArgument(2);

                    if (token >= leaderEventStore.firstToken) {
                        return forceReadFromLeader ? leaderEventStore : localEventStore;
                    }
                    return secondaryEventStore;
                });
        when(multiTierReaderEventStoreLocator.getEventStoreFromTimestamp(anyString(), anyBoolean(), anyLong()))
                .then((Answer<EventStore>) invocation -> {
                    boolean forceReadFromLeader = invocation.getArgument(1);
                    long timestamp = invocation.getArgument(2);

                    if (timestamp > System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1)) {
                        return forceReadFromLeader ? leaderEventStore : localEventStore;
                    }
                    return secondaryEventStore;
                });

        testSubject = new MultiTierEventStore(leaderEventStoreLocator,
                                              multiTierReaderEventStoreLocator,
                                              lowerTierEventStoreLocator);
    }

    @Test
    public void appendSnapshot() throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<Confirmation> result = testSubject.appendSnapshot(CONTEXT, Event.newBuilder().build());
        assertTrue(result.get(1, TimeUnit.SECONDS).getSuccess());
    }

    @Test
    public void createAppendEventConnection() throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<Confirmation> futureResult = new CompletableFuture<>();
        StreamObserver<InputStream> connection = testSubject.createAppendEventConnection(CONTEXT,
                                                                                         new CompletableStreamObserver<>(
                                                                                                 futureResult,
                                                                                                 "createAppendEventConnection",
                                                                                                 logger));
        connection.onNext(new ByteArrayInputStream(Event.newBuilder().build().toByteArray()));
        connection.onCompleted();

        assertTrue(futureResult.get(1, TimeUnit.SECONDS).getSuccess());
    }

    @Test
    public void listAggregateEventsFromMixed() throws InterruptedException {
        FakeStreamObserver<SerializedEvent> counter = new FakeStreamObserver<>();
        testSubject.listAggregateEvents(CONTEXT, GetAggregateEventsRequest.newBuilder()
                                                                          .setAggregateId(AGGREGATE_PARTLY_PRIMARY)
                                                                          .build(), counter);
        assertWithin(1, TimeUnit.SECONDS, () -> assertEquals(1, counter.completedCount()));
        counter.values().forEach(inputStream -> System.out.println(inputStream.asEvent()));
        assertEquals(11, counter.values().size());
    }

    @Test
    public void listAggregateEventsFromSnapshotFromSecondary() throws InterruptedException {
        FakeStreamObserver<SerializedEvent> counter = new FakeStreamObserver<>();
        testSubject.listAggregateEvents(CONTEXT, GetAggregateEventsRequest.newBuilder()
                                                                          .setAllowSnapshots(true)
                                                                          .setAggregateId(
                                                                                  AGGREGATE_PARTLY_PRIMARY_MORE_RECENT_SNAPSHOT_SECONDARY)
                                                                          .build(), counter);
        assertWithin(1, TimeUnit.SECONDS, () -> assertEquals(1, counter.completedCount()));
        counter.values().forEach(inputStream -> System.out.println(inputStream.asEvent()));
        assertEquals(3, counter.values().size());
    }

    @Test
    public void listAggregateEventsWithSnapshotInPrimary()
            throws InterruptedException {
        FakeStreamObserver<SerializedEvent> counter = new FakeStreamObserver<>();
        testSubject.listAggregateEvents(CONTEXT, GetAggregateEventsRequest.newBuilder()
                                                                          .setAggregateId(
                                                                                  AGGREGATE_PARTLY_PRIMARY_SNAPSHOT_PRIMARY)
                                                                          .setAllowSnapshots(true)
                                                                          .build(), counter);
        assertWithin(1, TimeUnit.SECONDS, () -> assertEquals(1, counter.completedCount()));
        assertEquals(4, counter.values().size());
    }

    @Test
    public void listAggregateEventsWithSnapshotInSecondary()
            throws InterruptedException {
        FakeStreamObserver<SerializedEvent> counter = new FakeStreamObserver<>();
        testSubject.listAggregateEvents(CONTEXT, GetAggregateEventsRequest.newBuilder()
                                                                          .setAggregateId(
                                                                                  AGGREGATE_PARTLY_PRIMARY_SNAPSHOT_SECONDARY)
                                                                          .setAllowSnapshots(true)
                                                                          .build(), counter);
        assertWithin(1, TimeUnit.SECONDS, () -> assertEquals(1, counter.completedCount()));
        assertEquals(8, counter.values().size());
    }

    @Test
    public void listAggregateEventsFromSecondary() throws ExecutionException, InterruptedException, TimeoutException {
        FakeStreamObserver<SerializedEvent> counter = new FakeStreamObserver<>();
        testSubject.listAggregateEvents(CONTEXT, GetAggregateEventsRequest.newBuilder()
                                                                          .setAggregateId(AGGREGATE_ONLY_SECONDARY)
                                                                          .build(), counter);
        assertWithin(1, TimeUnit.SECONDS, () -> assertEquals(1, counter.completedCount()));
        assertEquals(11, counter.values().size());
    }

    @Test
    public void listAggregateEventsFromLeader() throws InterruptedException {
        FakeStreamObserver<SerializedEvent> counter = new FakeStreamObserver<>();
        testSubject.listAggregateEvents(CONTEXT, GetAggregateEventsRequest.newBuilder()
                                                                          .setAggregateId(AGGREGATE_FULLY_PRIMARY)
                                                                          .build(), counter);
        assertWithin(1, TimeUnit.SECONDS, () -> assertEquals(1, counter.completedCount()));
        assertEquals(11, counter.values().size());
    }

    @Test
    public void listSnapshotEventsFromLeader() throws ExecutionException, InterruptedException, TimeoutException {
        FakeStreamObserver<SerializedEvent> counter = new FakeStreamObserver<>();
        testSubject.listAggregateSnapshots(CONTEXT, GetAggregateSnapshotsRequest.newBuilder()
                                                                                .setAggregateId(
                                                                                        AGGREGATE_PARTLY_PRIMARY_SNAPSHOT_PRIMARY)
                                                                                .build(), counter);
        assertWithin(1, TimeUnit.SECONDS, () -> assertEquals(1, counter.completedCount()));
        assertEquals(1, counter.values().size());
    }

    @Test
    public void listSnapshotEventsFromSecondary() throws ExecutionException, InterruptedException, TimeoutException {
        FakeStreamObserver<SerializedEvent> counter = new FakeStreamObserver<>();
        testSubject.listAggregateSnapshots(CONTEXT, GetAggregateSnapshotsRequest.newBuilder()
                                                                                .setAggregateId(
                                                                                        AGGREGATE_PARTLY_PRIMARY_SNAPSHOT_SECONDARY)
                                                                                .build(), counter);
        assertWithin(1, TimeUnit.SECONDS, () -> assertEquals(1, counter.completedCount()));
        assertEquals(1, counter.values().size());
    }

    @Test
    public void listEvents() throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<Event> firstEvent = new CompletableFuture<>();
        StreamObserver<GetEventsRequest> requestStream = testSubject.listEvents(CONTEXT,
                                                                                new StreamObserver<InputStream>() {
                                                                                    @Override
                                                                                    public void onNext(
                                                                                            InputStream inputStream) {
                                                                                        try {
                                                                                            firstEvent
                                                                                                    .complete(Event.parseFrom(
                                                                                                            inputStream));
                                                                                        } catch (IOException e) {
                                                                                            firstEvent
                                                                                                    .completeExceptionally(
                                                                                                            e);
                                                                                        }
                                                                                    }

                                                                                    @Override
                                                                                    public void onError(
                                                                                            Throwable throwable) {
                                                                                        firstEvent
                                                                                                .completeExceptionally(
                                                                                                        throwable);
                                                                                    }

                                                                                    @Override
                                                                                    public void onCompleted() {

                                                                                    }
                                                                                });

        requestStream.onNext(GetEventsRequest.newBuilder()
                                             .setTrackingToken(150)
                                             .setForceReadFromLeader(false)
                                             .build());

        assertEquals(LOCAL, firstEvent.get(1, TimeUnit.SECONDS).getMessageIdentifier());
    }

    @Test
    public void getFirstToken() throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<TrackingToken> trackingTokenHolder = new CompletableFuture<>();
        testSubject.getFirstToken(CONTEXT,
                                  GetFirstTokenRequest.newBuilder()
                                                      .build(),
                                  new CompletableStreamObserver<>(trackingTokenHolder, "getFirstToken", logger));
        assertEquals(0, trackingTokenHolder.get(1, TimeUnit.SECONDS).getToken());
    }

    @Test
    public void getLastToken() throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<TrackingToken> trackingTokenHolder = new CompletableFuture<>();
        testSubject.getLastToken(CONTEXT,
                                 GetLastTokenRequest.newBuilder()
                                                    .build(),
                                 new CompletableStreamObserver<>(trackingTokenHolder, "getLastToken", logger));
        assertEquals(200, trackingTokenHolder.get(1, TimeUnit.SECONDS).getToken());
    }

    @Test
    public void getTokenAtRecent() throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<TrackingToken> futureResponse = new CompletableFuture<>();
        testSubject.getTokenAt(CONTEXT,
                               GetTokenAtRequest.newBuilder()
                                                .setInstant(Instant.now().minus(Duration.ofMinutes(10L)).toEpochMilli())
                                                .build(),
                               new CompletableStreamObserver<>(futureResponse, "getTokenAt", logger));
        assertEquals(100, futureResponse.get(1, TimeUnit.SECONDS).getToken());
    }

    @Test
    public void getTokenAtOld() throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<TrackingToken> futureResponse = new CompletableFuture<>();
        testSubject.getTokenAt(CONTEXT,
                               GetTokenAtRequest.newBuilder()
                                                .setInstant(Instant.now().minus(Duration.ofDays(10L)).toEpochMilli())
                                                .build(),
                               new CompletableStreamObserver<>(futureResponse, "getTokenAt", logger));
        assertEquals(0, futureResponse.get(1, TimeUnit.SECONDS).getToken());
    }

    @Test
    public void readHighestSequenceNrFromPrimary() throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<ReadHighestSequenceNrResponse> futureResponse = new CompletableFuture<>();
        testSubject.readHighestSequenceNr(CONTEXT,
                                          ReadHighestSequenceNrRequest.newBuilder()
                                                                      .setAggregateId(AGGREGATE_PARTLY_PRIMARY)
                                                                      .build(),
                                          new CompletableStreamObserver<>(futureResponse,
                                                                          "readHighestSequenceNr",
                                                                          logger));
        assertEquals(10, futureResponse.get(1, TimeUnit.SECONDS).getToSequenceNr());
    }

    @Test
    public void readHighestSequenceNrFromSecondary() throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<ReadHighestSequenceNrResponse> futureResponse = new CompletableFuture<>();
        testSubject.readHighestSequenceNr(CONTEXT,
                                          ReadHighestSequenceNrRequest.newBuilder()
                                                                      .setAggregateId(AGGREGATE_ONLY_SECONDARY)
                                                                      .build(),
                                          new CompletableStreamObserver<>(futureResponse,
                                                                          "readHighestSequenceNr",
                                                                          logger));
        assertEquals(10, futureResponse.get(1, TimeUnit.SECONDS).getToSequenceNr());
    }

    @Test
    public void queryEventsRecentEventsFromFollower()
            throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<QueryEventsResponse> firstResultHolder = new CompletableFuture<>();
        StreamObserver<QueryEventsRequest> requestStream = testSubject.queryEvents(CONTEXT,
                                                                                   new CompletableStreamObserver<>(
                                                                                           firstResultHolder,
                                                                                           "queryEvents",
                                                                                           logger));
        requestStream.onNext(QueryEventsRequest.newBuilder()
                                               .setQuery("last minute")
                                               .setForceReadFromLeader(false)
                                               .build());

        assertEquals(LOCAL, firstResultHolder.get(1, TimeUnit.SECONDS).getColumns().getColumn(0));
    }

    @Test
    public void queryEventsRecentEventsFromLeader() throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<QueryEventsResponse> firstResultHolder = new CompletableFuture<>();
        StreamObserver<QueryEventsRequest> requestStream = testSubject.queryEvents(CONTEXT,
                                                                                   new CompletableStreamObserver<>(
                                                                                           firstResultHolder,
                                                                                           "queryEvents",
                                                                                           logger));
        requestStream.onNext(QueryEventsRequest.newBuilder()
                                               .setQuery("last minute")
                                               .setForceReadFromLeader(true)
                                               .build());

        assertEquals(LEADER, firstResultHolder.get(1, TimeUnit.SECONDS).getColumns().getColumn(0));
    }

    @Test
    public void queryEventsOldEventsFromSecondary() throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<QueryEventsResponse> firstResultHolder = new CompletableFuture<>();
        StreamObserver<QueryEventsRequest> requestStream = testSubject.queryEvents(CONTEXT,
                                                                                   new CompletableStreamObserver<>(
                                                                                           firstResultHolder,
                                                                                           "queryEvents",
                                                                                           logger));
        requestStream.onNext(QueryEventsRequest.newBuilder()
                                               .setQuery("last day")
                                               .setForceReadFromLeader(true)
                                               .build());

        assertEquals(SECONDARY, firstResultHolder.get(1, TimeUnit.SECONDS).getColumns().getColumn(0));
    }

    private class MockEventStore implements EventStore {

        private final long firstToken;
        protected final long lastToken;
        protected final long lastSnapshotToken = -1;
        private final String label;
        private final Map<String, Long> lastSequenceNumberPerAggregate = new ConcurrentHashMap<>();
        private final Map<String, Long> firstAggregateSequenceNumber = new ConcurrentHashMap<>();
        private final Map<String, Long> aggregateSnapshotIndex = new ConcurrentHashMap<>();

        protected MockEventStore(long firstToken, long lastToken, String label) {
            this.firstToken = firstToken;
            this.lastToken = lastToken;
            this.label = label;
        }

        protected MockEventStore withLastAggregateSequenceNumber(String aggregateIdentifier, long sequenceNumber) {
            lastSequenceNumberPerAggregate.put(aggregateIdentifier, sequenceNumber);
            return this;
        }

        protected MockEventStore withFirstAggregateSequenceNumber(String aggregateIdentifier, long sequenceNumber) {
            firstAggregateSequenceNumber.put(aggregateIdentifier, sequenceNumber);
            return this;
        }

        protected MockEventStore withSnapshot(String aggregateIdentifier, long sequenceNumber) {
            aggregateSnapshotIndex.put(aggregateIdentifier, sequenceNumber);
            return this;
        }


        @Override
        public CompletableFuture<Confirmation> appendSnapshot(String context, Event eventMessage) {
            return CompletableFuture.completedFuture(Confirmation.newBuilder().setSuccess(true).build());
        }

        @Override
        public StreamObserver<InputStream> createAppendEventConnection(String context,
                                                                       StreamObserver<Confirmation> responseObserver) {
            return new StreamObserver<InputStream>() {
                @Override
                public void onNext(InputStream inputStream) {

                }

                @Override
                public void onError(Throwable throwable) {

                }

                @Override
                public void onCompleted() {
                    responseObserver.onNext(Confirmation.newBuilder()
                                                        .setSuccess(true)
                                                        .build());
                    responseObserver.onCompleted();
                }
            };
        }

        @Override
        public void listAggregateEvents(String context, GetAggregateEventsRequest request,
                                        StreamObserver<SerializedEvent> responseObserver) {
            long from = firstAggregateSequenceNumber.getOrDefault(request.getAggregateId(), -1L);
            long to = lastSequenceNumberPerAggregate.getOrDefault(request.getAggregateId(), -1L) + 1;
            long snapshot = aggregateSnapshotIndex.getOrDefault(request.getAggregateId(), -1L);

            if (request.getMaxSequence() > 0) {
                to = Math.min(to, request.getMaxSequence());
            }

            if (request.getAllowSnapshots() && snapshot >= 0) {
                System.out.printf("%s: sending snapshot %d%n", label, snapshot);
                Event event = Event.newBuilder()
                                   .setAggregateSequenceNumber(snapshot)
                                   .setAggregateIdentifier(request.getAggregateId())
                                   .setSnapshot(true)
                                   .build();
                responseObserver.onNext(new SerializedEvent(event));
                from = snapshot + 1;
            }
            if (to >= 0) {
                LongStream.range(from, to).forEach(seq -> {
                    System.out.printf("%s: sending %d%n", label, seq);
                    Event event = Event.newBuilder().setAggregateSequenceNumber(seq)
                                       .setAggregateIdentifier(request.getAggregateId()).build();
                    responseObserver.onNext(new SerializedEvent(event));
                });
            }
            System.out.printf("%s: completed%n", label);
            responseObserver.onCompleted();
        }

        @Override
        public StreamObserver<GetEventsRequest> listEvents(String context,
                                                           StreamObserver<InputStream> responseStreamObserver) {
            return new StreamObserver<GetEventsRequest>() {
                @Override
                public void onNext(GetEventsRequest getEventsRequest) {
                    Event event = Event.newBuilder()
                                       .setMessageIdentifier(label)
                                       .build();
                    responseStreamObserver.onNext(new SerializedEvent(event).asInputStream());
                }

                @Override
                public void onError(Throwable throwable) {
                    responseStreamObserver.onCompleted();
                }

                @Override
                public void onCompleted() {
                    responseStreamObserver.onCompleted();
                }
            };
        }

        @Override
        public void getFirstToken(String context, GetFirstTokenRequest request,
                                  StreamObserver<TrackingToken> responseObserver) {

            responseObserver.onNext(TrackingToken.newBuilder()
                                                 .setToken(firstToken)
                                                 .build());
            responseObserver.onCompleted();
        }

        @Override
        public void getLastToken(String context, GetLastTokenRequest request,
                                 StreamObserver<TrackingToken> responseObserver) {
            responseObserver.onNext(TrackingToken.newBuilder()
                                                 .setToken(lastToken)
                                                 .build());

            responseObserver.onCompleted();
        }

        @Override
        public void getTokenAt(String context, GetTokenAtRequest request,
                               StreamObserver<TrackingToken> responseObserver) {

            responseObserver.onNext(TrackingToken.newBuilder()
                                                 .setToken(firstToken)
                                                 .build());
            responseObserver.onCompleted();
        }

        @Override
        public void readHighestSequenceNr(String context, ReadHighestSequenceNrRequest request,
                                          StreamObserver<ReadHighestSequenceNrResponse> responseObserver) {
            responseObserver.onNext(
                    ReadHighestSequenceNrResponse.newBuilder()
                                                 .setToSequenceNr(
                                                         lastSequenceNumberPerAggregate
                                                                 .getOrDefault(request.getAggregateId(), -1L))
                                                 .build());

            responseObserver.onCompleted();
        }

        @Override
        public StreamObserver<QueryEventsRequest> queryEvents(String context,
                                                              StreamObserver<QueryEventsResponse> responseObserver) {
            return new StreamObserver<QueryEventsRequest>() {
                @Override
                public void onNext(QueryEventsRequest queryEventsRequest) {
                    responseObserver.onNext(QueryEventsResponse.newBuilder()
                                                               .setColumns(ColumnsResponse.newBuilder()
                                                                                          .addColumn(label)
                                                                                          .build())
                                                               .build());
                }

                @Override
                public void onError(Throwable throwable) {
                    responseObserver.onCompleted();
                }

                @Override
                public void onCompleted() {
                    responseObserver.onCompleted();
                }
            };
        }

        @Override
        public void listAggregateSnapshots(String context, GetAggregateSnapshotsRequest request,
                                           StreamObserver<SerializedEvent> responseObserver) {
            long snapshot = aggregateSnapshotIndex.getOrDefault(request.getAggregateId(), -1L);
            if (snapshot > 0) {
                Event event = Event.newBuilder().setAggregateSequenceNumber(snapshot)
                                   .setAggregateIdentifier(request.getAggregateId()).build();
                responseObserver.onNext(new SerializedEvent(event));
            }
            responseObserver.onCompleted();
        }

        @Override
        public void deleteAllEventData(String context) {

        }
    }

    private class MockLowerTierEventStore extends MockEventStore implements LowerTierEventStore {

        private MockLowerTierEventStore(long firstToken, long lastToken, String label) {
            super(firstToken, lastToken, label);
        }

        @Override
        public Flux<TransactionWithToken> eventTransactions(String context, long from, long to) {
            return Flux.empty();
        }

        @Override
        public Flux<TransactionWithToken> snapshotTransactions(String context, long from, long to) {
            return Flux.empty();
        }

        @Override
        public Optional<Long> getHighestSequenceNr(String context, String aggregateId, int maxSegments, long maxToken) {
            return Optional.empty();
        }

        @Override
        public long getLastToken(String context) {
            return lastToken;
        }

        @Override
        public long getLastSnapshotToken(String context) {
            return lastSnapshotToken;
        }
    }
}
