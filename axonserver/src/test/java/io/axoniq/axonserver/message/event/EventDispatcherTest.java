package io.axoniq.axonserver.message.event;

import io.axoniq.axonserver.grpc.event.Confirmation;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.EventWithToken;
import io.axoniq.axonserver.grpc.event.GetEventsRequest;
import io.axoniq.axonserver.metric.DefaultMetricCollector;
import io.axoniq.axonserver.topology.EventStoreLocator;
import io.axoniq.axonserver.topology.Topology;
import io.axoniq.axonserver.util.CountingStreamObserver;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.Metrics;
import org.junit.*;
import org.junit.runner.*;
import org.mockito.*;
import org.mockito.runners.*;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.*;

/**
 * Author: marc
 */
@RunWith(MockitoJUnitRunner.class)
public class EventDispatcherTest {
    private EventDispatcher testSubject;
    @Mock
    private EventStore eventStoreClient;

    @Mock
    private EventStoreLocator eventStoreManager;

    @Mock
    private StreamObserver<InputStream> appendEventConnection;

    @Before
    public void setUp() {
        when(eventStoreClient.createAppendEventConnection(any(), any())).thenReturn(appendEventConnection);
        when(eventStoreManager.getEventStore(any())).thenReturn(eventStoreClient);
        testSubject = new EventDispatcher(eventStoreManager, Optional.empty(), () -> Topology.DEFAULT_CONTEXT,
                                          Metrics.globalRegistry,
                                          new DefaultMetricCollector());
    }

    @Test
    public void appendEvent() {
        CountingStreamObserver<Confirmation> responseObserver = new CountingStreamObserver<>();
        StreamObserver<InputStream> inputStream = testSubject.appendEvent(responseObserver);
        inputStream.onNext(dummyEvent());
        assertEquals(0, responseObserver.count);
        inputStream.onCompleted();
        verify( appendEventConnection).onCompleted();
    }

    private InputStream dummyEvent() {
        return new ByteArrayInputStream(Event.newBuilder().build().toByteArray());
    }

    @Test
    public void appendEventRollback() {
        CountingStreamObserver<Confirmation> responseObserver = new CountingStreamObserver<>();
        StreamObserver<InputStream> inputStream = testSubject.appendEvent(responseObserver);
        inputStream.onNext(dummyEvent());
        assertEquals(0, responseObserver.count);
        inputStream.onError(new Throwable());
        assertNull(responseObserver.error);
        verify( appendEventConnection).onError(anyObject());
    }

    @Test
    public void appendSnapshot() {
        CountingStreamObserver<Confirmation> responseObserver = new CountingStreamObserver<>();
        CompletableFuture<Confirmation> appendFuture = new CompletableFuture<>();
        when(eventStoreClient.appendSnapshot(any(), any(Event.class))).thenReturn(appendFuture);
        testSubject.appendSnapshot(Event.newBuilder().build(), responseObserver);
        appendFuture.complete(Confirmation.newBuilder().build());
        verify(eventStoreClient).appendSnapshot(any(), any(Event.class));
        assertEquals(1, responseObserver.count);
    }

    @Test
    public void listAggregateEvents() {
    }

    @Test
    public void listEvents() {
        CountingStreamObserver<InputStream> responseObserver = new CountingStreamObserver<>();
        AtomicReference<StreamObserver<InputStream>> eventStoreOutputStreamRef = new AtomicReference<>();
        StreamObserver<GetEventsRequest> eventStoreResponseStream = new StreamObserver<GetEventsRequest>() {
            @Override
            public void onNext(GetEventsRequest o) {
                StreamObserver<InputStream> responseStream = eventStoreOutputStreamRef.get();
                responseStream.onNext(new ByteArrayInputStream(EventWithToken.newBuilder().build().toByteArray()));
                responseStream.onCompleted();
            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onCompleted() {

            }
        };
        when(eventStoreClient.listEvents(any(), any(StreamObserver.class))).then(a -> {
            eventStoreOutputStreamRef.set((StreamObserver<InputStream>) a.getArguments()[1]);
            return eventStoreResponseStream;
        });
        StreamObserver<GetEventsRequest> inputStream = testSubject.listEvents(responseObserver);
        inputStream.onNext(GetEventsRequest.newBuilder().build());
        assertEquals(1, responseObserver.count);
        assertTrue(responseObserver.completed);
    }


}