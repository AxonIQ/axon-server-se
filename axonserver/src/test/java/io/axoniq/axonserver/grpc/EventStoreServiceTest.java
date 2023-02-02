package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.AxonServerAccessController;
import io.axoniq.axonserver.config.GrpcContextAuthenticationProvider;
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
import io.axoniq.axonserver.localstorage.SerializedEvent;
import io.axoniq.axonserver.localstorage.SerializedEventWithToken;
import io.axoniq.axonserver.message.event.EventDispatcher;
import io.axoniq.axonserver.test.FakeStreamObserver;
import io.axoniq.axonserver.topology.Topology;
import io.grpc.stub.StreamObserver;
import org.junit.Before;
import org.junit.Test;
import org.springframework.security.core.Authentication;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static io.axoniq.axonserver.test.AssertUtils.assertWithin;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class EventStoreServiceTest {

    private EventStoreService testSubject;
    private Authentication authentication = GrpcContextAuthenticationProvider.USER_PRINCIPAL;

    private final Set<String> allowedContext = new HashSet<>();

    @Before
    public void setUp() throws Exception {
        testSubject = new EventStoreService(() -> Topology.DEFAULT_CONTEXT,
                                            () -> authentication,
                                            eventDispatcher(), Executors::newSingleThreadExecutor, accessController());
    }

    private AxonServerAccessController accessController() {
        return new AxonServerAccessController() {
            @Override
            public boolean allowed(String fullMethodName, String context, String token) {
                return false;
            }

            @Override
            public boolean allowed(String fullMethodName, String context, Authentication authentication) {
                return allowedContext.contains(context);
            }

            @Override
            public Authentication authentication(String context, String token) {
                return null;
            }
        };
    }

    private EventDispatcher eventDispatcher() {
        EventDispatcher eventDispatcher = mock(EventDispatcher.class);
        when(eventDispatcher.appendEvent(anyString(), any(), any())).thenAnswer(invocationOnMock -> Mono.create(
                sink -> {
                    Flux<?> events = invocationOnMock.getArgument(2);
                    events.subscribe(e -> {}, sink::error, sink::success);
                }

        ));
        when(eventDispatcher.appendSnapshot(anyString(), any(), any())).thenReturn(Mono.empty());

        when(eventDispatcher.queryEvents(anyString(), any(), any())).thenAnswer(invocationOnMock -> Flux.create(sink -> {
            Flux<QueryEventsRequest> requests = invocationOnMock.getArgument(2);
            requests.subscribe(req -> {

            }, err -> sink.complete(), sink::complete );

        }));

        when(eventDispatcher.aggregateSnapshots(anyString(), any(), any())).thenReturn(Flux.just(serializedEvent()));
        when(eventDispatcher.aggregateEvents(anyString(), any(), any())).thenReturn(Flux.just(serializedEvent()));
        when(eventDispatcher.events(anyString(), any(), any())).thenAnswer(invocationOnMock -> Flux.create(sink -> {
            Flux<GetEventsRequest> requests = invocationOnMock.getArgument(2);
            requests.subscribe(r -> {
                sink.next(new SerializedEventWithToken(0, serializedEvent()));
            }, error -> sink.complete(), sink::complete);
        }));

        when(eventDispatcher.firstEventToken(anyString())).thenReturn(Mono.just(100L));
        when(eventDispatcher.eventTokenAt(anyString(), any())).thenReturn(Mono.just(200L));
        when(eventDispatcher.lastEventToken(anyString())).thenReturn(Mono.just(300L));
        when(eventDispatcher.highestSequenceNumber(anyString(), anyString())).thenReturn(Mono.just(12L));

        return eventDispatcher;
    }

    private SerializedEvent serializedEvent() {
        return new SerializedEvent(Event.getDefaultInstance());
    }

    @Test
    public void appendEvent() {
        FakeStreamObserver<Confirmation> responseObserver = new FakeStreamObserver<>();
        StreamObserver<SerializedEvent> requestStream = testSubject.appendEvent(
                responseObserver);
        requestStream.onNext(serializedEvent());
        requestStream.onCompleted();
        assertEquals(1, responseObserver.values().size());
    }

    @Test
    public void appendSnapshot() {
        FakeStreamObserver<Confirmation> responseObserver = new FakeStreamObserver<>();
        testSubject.appendSnapshot(Event.getDefaultInstance(), responseObserver);
        assertEquals(1, responseObserver.values().size());
    }

    @Test
    public void listAggregateEvents() throws InterruptedException {
        FakeStreamObserver<SerializedEvent> responseObserver = new FakeStreamObserver<>();
        testSubject.listAggregateEvents(GetAggregateEventsRequest.getDefaultInstance(), responseObserver);
        assertEquals(0, responseObserver.values().size());
        responseObserver.setIsReady(true);
        assertWithin(1, TimeUnit.SECONDS, () -> assertEquals(1, responseObserver.completedCount()));
        assertEquals(1, responseObserver.values().size());
    }

    @Test
    public void listEvents() {
        FakeStreamObserver<SerializedEventWithToken> responseObserver = new FakeStreamObserver<>();
        responseObserver.setIsReady(true);
        StreamObserver<GetEventsRequest> requestStream = testSubject.listEvents(
                responseObserver);

        requestStream.onNext(GetEventsRequest.getDefaultInstance());
        assertEquals(1, responseObserver.values().size());
        requestStream.onError(new RuntimeException("Client cancelled event processor"));
    }

    @Test
    public void getFirstToken() {
        FakeStreamObserver<TrackingToken> responseObserver = new FakeStreamObserver<>();
        testSubject.getFirstToken(GetFirstTokenRequest.getDefaultInstance(), responseObserver);
        assertEquals(1, responseObserver.values().size());
        assertEquals(100L, responseObserver.values().get(0).getToken());
        assertEquals(1, responseObserver.completedCount());
    }

    @Test
    public void getLastToken() {
        FakeStreamObserver<TrackingToken> responseObserver = new FakeStreamObserver<>();
        testSubject.getLastToken(GetLastTokenRequest.getDefaultInstance(), responseObserver);
        assertEquals(1, responseObserver.values().size());
        assertEquals(300L, responseObserver.values().get(0).getToken());
        assertEquals(1, responseObserver.completedCount());
    }

    @Test
    public void getTokenAt() {
        FakeStreamObserver<TrackingToken> responseObserver = new FakeStreamObserver<>();
        testSubject.getTokenAt(GetTokenAtRequest.getDefaultInstance(), responseObserver);
        assertEquals(1, responseObserver.values().size());
        assertEquals(200L, responseObserver.values().get(0).getToken());
        assertEquals(1, responseObserver.completedCount());
    }

    @Test
    public void readHighestSequenceNr() {
        FakeStreamObserver<ReadHighestSequenceNrResponse> responseObserver = new FakeStreamObserver<>();
        testSubject.readHighestSequenceNr(ReadHighestSequenceNrRequest.getDefaultInstance(), responseObserver);
        assertEquals(1, responseObserver.values().size());
        assertEquals(12L, responseObserver.values().get(0).getToSequenceNr());
        assertEquals(1, responseObserver.completedCount());
    }

    @Test
    public void queryEvents() {
        allowedContext.add(Topology.DEFAULT_CONTEXT);

        FakeStreamObserver<QueryEventsResponse> streamObserver = new FakeStreamObserver<>();
        StreamObserver<QueryEventsRequest> requestStream = testSubject.queryEvents(streamObserver);
        requestStream.onNext(QueryEventsRequest.newBuilder()
                                               .build());
        requestStream.onCompleted();

        assertEquals(1, streamObserver.completedCount());
    }

    @Test
    public void queryEventsUnauthorizedContext() {
        allowedContext.add(Topology.DEFAULT_CONTEXT);

        FakeStreamObserver<QueryEventsResponse> streamObserver = new FakeStreamObserver<>();
        StreamObserver<QueryEventsRequest> requestStream = testSubject.queryEvents(streamObserver);
        requestStream.onNext(QueryEventsRequest.newBuilder()
                                     .setContextName("OTHER")
                                               .build());
        requestStream.onCompleted();

        assertEquals(1, streamObserver.errors().size());
    }
    @Test
    public void queryEventsNoAccessControl() {
        allowedContext.add(Topology.DEFAULT_CONTEXT);
        authentication = GrpcContextAuthenticationProvider.DEFAULT_PRINCIPAL;

        FakeStreamObserver<QueryEventsResponse> streamObserver = new FakeStreamObserver<>();
        StreamObserver<QueryEventsRequest> requestStream = testSubject.queryEvents(streamObserver);
        requestStream.onNext(QueryEventsRequest.newBuilder()
                                               .setContextName("OTHER")
                                               .build());
        requestStream.onCompleted();

        assertEquals(1, streamObserver.completedCount());
    }

    @Test
    public void listAggregateSnapshots() throws InterruptedException {
        FakeStreamObserver<SerializedEvent> responseObserver = new FakeStreamObserver<>();
        testSubject.listAggregateSnapshots(GetAggregateSnapshotsRequest.getDefaultInstance(), responseObserver);
        assertEquals(0, responseObserver.values().size());
        responseObserver.setIsReady(true);
        assertWithin(1, TimeUnit.SECONDS, () -> assertEquals(1, responseObserver.completedCount()));
        assertEquals(1, responseObserver.values().size());
    }
}