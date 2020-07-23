package io.axoniq.axonserver.enterprise.replication.snapshot;

import io.axoniq.axonserver.cluster.replication.DefaultSnapshotContext;
import io.axoniq.axonserver.cluster.snapshot.SnapshotContext;
import io.axoniq.axonserver.enterprise.replication.group.ReplicationGroupController;
import io.axoniq.axonserver.enterprise.storage.multitier.LowerTierEventStoreLocator;
import io.axoniq.axonserver.grpc.cluster.Role;
import io.axoniq.axonserver.grpc.cluster.SerializedObject;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import io.axoniq.axonserver.localstorage.SerializedEvent;
import io.axoniq.axonserver.localstorage.SerializedTransactionWithToken;
import org.junit.*;
import org.springframework.data.util.CloseableIterator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author Marc Gathier
 */
public class SnapshotTransactionsSnapshotDataStoreTest {

    private SnapshotTransactionsSnapshotDataStore testSubject;

    @Before
    public void setUp() {
        LocalEventStore localEventStore = mock(LocalEventStore.class);
        when(localEventStore.getLastSnapshot(anyString())).thenReturn(10L);
        when(localEventStore.snapshotTransactionsIterator(anyString(), anyLong(), anyLong())).then(invocation -> {
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

        ReplicationGroupController replicationGroupController = mock(ReplicationGroupController.class);
        when(replicationGroupController.getContextNames(anyString())).thenReturn(Arrays.asList("dummy", "dummy2"));
        LowerTierEventStoreLocator lowerTierEventStoreLocator = mock(LowerTierEventStoreLocator.class);
        testSubject = new SnapshotTransactionsSnapshotDataStore("dummy",
                                                                localEventStore,
                                                                lowerTierEventStoreLocator, replicationGroupController);
    }

    @Test
    public void streamSnapshotData() {
        Map<String, Long> lastSnapshotTokenMap = new HashMap<>();
        lastSnapshotTokenMap.put("dummy", 5L);
        lastSnapshotTokenMap.put("dummy2", 5L);
        SnapshotContext installationContext = new DefaultSnapshotContext(Collections.emptyMap(),
                                                                         lastSnapshotTokenMap,
                                                                         true, Role.PRIMARY);
        List<SerializedObject> messages = testSubject.streamSnapshotData(installationContext).collectList().block();
        assertEquals(0, messages.size());
        messages = testSubject.streamAppendOnlyData(installationContext).collectList().block();
        assertEquals(10, messages.size());
    }

    @Test
    public void streamSnapshotDataPeerWithoutReplicationGroups() {
        Map<String, Long> lastSnapshotTokenMap = new HashMap<>();
        lastSnapshotTokenMap.put("dummy", 5L);
        lastSnapshotTokenMap.put("dummy2", 5L);
        SnapshotContext installationContext = new DefaultSnapshotContext(Collections.emptyMap(),
                                                                         lastSnapshotTokenMap,
                                                                         false, Role.PRIMARY);
        List<SerializedObject> messages = new ArrayList<>();
        testSubject.streamSnapshotData(installationContext).subscribe(c -> messages.add(c));
        assertEquals(5, messages.size());
        testSubject.streamAppendOnlyData(installationContext).subscribe(c -> messages.add(c));
        assertEquals(5, messages.size());
    }
}