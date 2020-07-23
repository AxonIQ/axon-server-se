package io.axoniq.axonserver.enterprise.messaging.event;

import com.google.protobuf.ByteString;
import io.axoniq.axonserver.enterprise.cluster.CompletableStreamObserver;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.TrackingToken;
import io.axoniq.axonserver.grpc.internal.GetHighestSequenceNumberRequest;
import io.axoniq.axonserver.grpc.internal.GetHighestSequenceNumberResponse;
import io.axoniq.axonserver.grpc.internal.GetLastSnapshotTokenRequest;
import io.axoniq.axonserver.grpc.internal.GetTransactionsRequest;
import io.axoniq.axonserver.grpc.internal.TransactionWithToken;
import io.axoniq.axonserver.localstorage.EventType;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import io.axoniq.axonserver.localstorage.SerializedEvent;
import io.axoniq.axonserver.localstorage.SerializedTransactionWithToken;
import io.grpc.stub.StreamObserver;
import org.junit.*;
import org.springframework.data.util.CloseableIterator;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author Marc Gathier
 */
public class LowerTierEventStoreServiceTest {

    private LowerTierEventStoreService testSubject;

    @Before
    public void setup() {
        LocalEventStore localEventStore = mock(LocalEventStore.class);
        when(localEventStore.getHighestSequenceNr(anyString(), anyString(), anyInt(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(10L));
        when(localEventStore.eventTransactionsIterator(anyString(), anyLong(), anyLong()))
                .then(invocation -> createIterator(invocation.getArgument(1),
                                                   invocation.getArgument(2),
                                                   EventType.EVENT));
        when(localEventStore.snapshotTransactionsIterator(anyString(), anyLong(), anyLong()))
                .then(invocation -> createIterator(invocation.getArgument(1),
                                                   invocation.getArgument(2),
                                                   EventType.SNAPSHOT));
        when(localEventStore.getLastSnapshot(anyString())).thenReturn(10L);
        testSubject = new LowerTierEventStoreService(localEventStore);
    }

    private CloseableIterator<SerializedTransactionWithToken> createIterator(long from, long to, EventType type) {
        AtomicLong current = new AtomicLong(from);

        return new CloseableIterator<SerializedTransactionWithToken>() {
            @Override
            public void close() {

            }

            @Override
            public boolean hasNext() {
                return current.get() < to;
            }

            @Override
            public SerializedTransactionWithToken next() {
                Event event = Event.newBuilder().setAggregateType(type.name()).setSnapshot(EventType.SNAPSHOT
                                                                                                   .equals(type))
                                   .build();
                return new SerializedTransactionWithToken(current.getAndIncrement(),
                                                          (byte) 0,
                                                          Collections.singletonList(new SerializedEvent(event)));
            }
        };
    }


    @Test
    public void listEvents() throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<Integer> eventsRead = new CompletableFuture<>();
        AtomicLong permits = new AtomicLong(2);
        StreamObserver<GetTransactionsRequest>[] requestStreams = new StreamObserver[1];
        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

        requestStreams[0] = testSubject
                .listEvents(new StreamObserver<TransactionWithToken>() {
                    int counter = 0;

                    @Override
                    public void onNext(TransactionWithToken transactionWithToken) {
                        scheduledExecutorService.execute(() -> {
                            for (ByteString bytes : transactionWithToken.getEventsList()) {
                                SerializedEvent event = new SerializedEvent(bytes.toByteArray());
                                if (event.asEvent().getSnapshot()) {
                                    eventsRead
                                            .completeExceptionally(new RuntimeException(
                                                    "Found snapshot, expecting event"));
                                }
                                counter++;
                            }
                            if (permits.decrementAndGet() <= 0) {
                                permits.set(2);
                                requestStreams[0].onNext(GetTransactionsRequest.newBuilder()
                                                                               .setNumberOfPermits(permits.get())
                                                                               .build());
                            }
                        });
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        eventsRead.completeExceptionally(throwable);
                    }

                    @Override
                    public void onCompleted() {
                        scheduledExecutorService.execute(() -> eventsRead.complete(counter));
                    }
                });
        requestStreams[0].onNext(GetTransactionsRequest.newBuilder().setTrackingToken(10).setToTrackingToken(20)
                                                       .setNumberOfPermits(permits.get()).build());
        int read = eventsRead.get(1, TimeUnit.SECONDS);
        assertEquals(10, read);
    }

    @Test
    public void listSnapshots() throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<Integer> eventsRead = new CompletableFuture<>();
        StreamObserver<GetTransactionsRequest> requestStream = testSubject
                .listSnapshots(new StreamObserver<TransactionWithToken>() {
                    int counter = 0;

                    @Override
                    public void onNext(TransactionWithToken transactionWithToken) {
                        for (ByteString bytes : transactionWithToken.getEventsList()) {
                            SerializedEvent event = new SerializedEvent(bytes.toByteArray());
                            if (!event.asEvent().getSnapshot()) {
                                eventsRead
                                        .completeExceptionally(new RuntimeException("Found event, expecting snapshot"));
                            }
                            counter++;
                        }
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        eventsRead.completeExceptionally(throwable);
                    }

                    @Override
                    public void onCompleted() {
                        eventsRead.complete(counter);
                    }
                });
        requestStream.onNext(GetTransactionsRequest.newBuilder().setTrackingToken(10).setToTrackingToken(20)
                                                   .setNumberOfPermits(100).build());
        int read = eventsRead.get(1, TimeUnit.SECONDS);
        assertEquals(10, read);
    }

    @Test
    public void getHighestSequenceNumber() throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<GetHighestSequenceNumberResponse> completableResponse = new CompletableFuture<>();
        testSubject.getHighestSequenceNumber(GetHighestSequenceNumberRequest.newBuilder()
                                                                            .setAggregateIdentifier("sample")
                                                                            .build(),
                                             new CompletableStreamObserver<>(completableResponse,
                                                                             "getHighestSequenceNumber",
                                                                             null));
        GetHighestSequenceNumberResponse response = completableResponse.get(1, TimeUnit.SECONDS);
        assertEquals(10, response.getSequenceNumber());
    }

    @Test
    public void getLastSnapshotToken() throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<TrackingToken> completableResponse = new CompletableFuture<>();
        testSubject.getLastSnapshotToken(GetLastSnapshotTokenRequest.newBuilder()
                                                                    .build(),
                                         new CompletableStreamObserver<>(completableResponse,
                                                                         "getLastSnapshotToken",
                                                                         null));
        TrackingToken response = completableResponse.get(1, TimeUnit.SECONDS);
        assertEquals(10, response.getToken());
    }
}