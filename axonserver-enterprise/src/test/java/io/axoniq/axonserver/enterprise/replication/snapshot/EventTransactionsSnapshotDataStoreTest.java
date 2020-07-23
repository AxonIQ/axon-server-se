package io.axoniq.axonserver.enterprise.replication.snapshot;

import io.axoniq.axonserver.cluster.replication.DefaultSnapshotContext;
import io.axoniq.axonserver.cluster.snapshot.SnapshotContext;
import io.axoniq.axonserver.enterprise.messaging.event.RemoteLowerTierEventStore;
import io.axoniq.axonserver.enterprise.replication.group.ReplicationGroupController;
import io.axoniq.axonserver.enterprise.storage.multitier.LowerTierEventStoreLocator;
import io.axoniq.axonserver.grpc.cluster.Role;
import io.axoniq.axonserver.grpc.cluster.SerializedObject;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.TrackingToken;
import io.axoniq.axonserver.grpc.internal.TransactionWithToken;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import io.axoniq.axonserver.localstorage.SerializedEvent;
import io.axoniq.axonserver.localstorage.SerializedTransactionWithToken;
import io.grpc.stub.StreamObserver;
import org.junit.*;
import org.springframework.data.util.CloseableIterator;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.LongStream;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author Marc Gathier
 */
public class EventTransactionsSnapshotDataStoreTest {

    private final AtomicLong localFirstToken = new AtomicLong();
    private final AtomicLong secondaryLastToken = new AtomicLong();
    private EventTransactionsSnapshotDataStore testSubject;

    @Before
    public void setUp() {
        LocalEventStore localEventStore = mock(LocalEventStore.class);
        when(localEventStore.getLastEvent(anyString())).thenReturn(10L);
        doAnswer(invocation -> {
            StreamObserver<TrackingToken> stream = invocation.getArgument(2);
            stream.onNext(TrackingToken.newBuilder().setToken(localFirstToken.get()).build());
            stream.onCompleted();
            return null;
        }).when(localEventStore).getFirstToken(anyString(), any(), any());
        when(localEventStore.firstToken(anyString())).thenReturn(localFirstToken.get());
        when(localEventStore.eventTransactionsIterator(anyString(), anyLong(), anyLong())).then(invocation -> {
            long from = invocation.getArgument(1);
            long to = invocation.getArgument(2);
            List<SerializedTransactionWithToken> transactionWithTokens = new ArrayList<>();
            LongStream.range(from, to).forEach(i ->
                                                       transactionWithTokens.add(new SerializedTransactionWithToken(1,
                                                                                                                    (byte) 0,
                                                                                                                    Arrays.asList(
                                                                                                                            new SerializedEvent(
                                                                                                                                    Event.newBuilder()
                                                                                                                                         .build())))));

            return new CloseableIterator<SerializedTransactionWithToken>() {
                final Iterator<SerializedTransactionWithToken> iterator = transactionWithTokens.iterator();

                @Override
                public boolean hasNext() {
                    return iterator.hasNext();
                }

                @Override
                public SerializedTransactionWithToken next() {
                    return iterator.next();
                }

                @Override
                public void close() {

                }
            };
        });

        RemoteLowerTierEventStore lowerTierEventStore = mock(RemoteLowerTierEventStore.class);
        LowerTierEventStoreLocator lowerTierEventStoreLocator = mock(LowerTierEventStoreLocator.class);
        when(lowerTierEventStoreLocator.getEventStore(anyString())).thenReturn(lowerTierEventStore);

        doAnswer(invocation -> {
            StreamObserver<TrackingToken> stream = invocation.getArgument(2);
            stream.onNext(TrackingToken.newBuilder().setToken(secondaryLastToken.get()).build());
            stream.onCompleted();
            return null;
        }).when(lowerTierEventStore).getLastToken(anyString(), any(), any());

        when(lowerTierEventStore.eventTransactions(anyString(), anyLong(), anyLong()))
                .then(invocation -> {
                    String context = invocation.getArgument(0);
                    long from = invocation.getArgument(1);
                    long to = invocation.getArgument(2);

                    return Flux.fromStream(
                            LongStream.range(from, to)
                                      .mapToObj(i ->
                                                        TransactionWithToken.newBuilder()
                                                                            .setContext(context)
                                                                            .setToken(i)
                                                                            .addEvents(Event.newBuilder()
                                                                                            .setMessageIdentifier(UUID.randomUUID()
                                                                                                                      .toString())
                                                                                            .build()
                                                                                            .toByteString())));
                });

        ReplicationGroupController replicationGroupController = mock(ReplicationGroupController.class);
        when(replicationGroupController.getContextNames(anyString())).thenReturn(Arrays.asList("dummy", "dummy2"));
        testSubject = new EventTransactionsSnapshotDataStore("dummy",
                                                             localEventStore,
                                                             lowerTierEventStoreLocator,
                                                             replicationGroupController);
    }

    @Test
    public void streamSnapshotData() {
        Map<String, Long> lastEventTokenMap = new HashMap<>();
        lastEventTokenMap.put("dummy", 5L);
        lastEventTokenMap.put("dummy2", 5L);
        SnapshotContext installationContext = new DefaultSnapshotContext(lastEventTokenMap,
                                                                         Collections.emptyMap(),
                                                                         true, Role.PRIMARY);
        List<SerializedObject> messages = testSubject.streamSnapshotData(installationContext).collectList().block();
        assertEquals(0, messages.size());
        messages = testSubject.streamAppendOnlyData(installationContext).collectList().block();
        assertEquals(10, messages.size());
    }

    @Test
    public void streamSnapshotDataFromLowerTier() {
        Map<String, Long> lastEventTokenMap = new HashMap<>();
        lastEventTokenMap.put("dummy", -1L);
        lastEventTokenMap.put("dummy2", 11L);
        localFirstToken.set(5);
        secondaryLastToken.set(7);
        SnapshotContext installationContext = new DefaultSnapshotContext(lastEventTokenMap,
                                                                         Collections.emptyMap(),
                                                                         true, Role.PRIMARY);
        List<SerializedObject> messages = testSubject.streamSnapshotData(installationContext).collectList().block();
        assertEquals(0, messages.size());
        messages = testSubject.streamAppendOnlyData(installationContext)
                              .collectList()
                              .block();
        assertEquals(11, messages.size());
    }

    @Test
    public void streamSnapshotDataPeerWithoutReplicationGroups() {
        Map<String, Long> lastEventTokenMap = new HashMap<>();
        lastEventTokenMap.put("dummy", 5L);
        lastEventTokenMap.put("dummy2", 5L);
        SnapshotContext installationContext = new DefaultSnapshotContext(lastEventTokenMap,
                                                                         Collections.emptyMap(),
                                                                         false, Role.PRIMARY);
        List<SerializedObject> messages = testSubject.streamSnapshotData(installationContext).collectList().block();
        assertEquals(5, messages.size());
        messages = testSubject.streamAppendOnlyData(installationContext).collectList().block();
        assertEquals(0, messages.size());
    }
}