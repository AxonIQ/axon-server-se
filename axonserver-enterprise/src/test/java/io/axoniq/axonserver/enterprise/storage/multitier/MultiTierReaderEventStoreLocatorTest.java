package io.axoniq.axonserver.enterprise.storage.multitier;

import io.axoniq.axonserver.config.SystemInfoProvider;
import io.axoniq.axonserver.enterprise.cluster.manager.LeaderEventStoreLocator;
import io.axoniq.axonserver.enterprise.messaging.event.LowerTierEventStore;
import io.axoniq.axonserver.enterprise.replication.RaftGroupRepositoryManager;
import io.axoniq.axonserver.grpc.event.TrackingToken;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import io.axoniq.axonserver.localstorage.file.EmbeddedDBProperties;
import io.axoniq.axonserver.message.event.EventStore;
import io.grpc.stub.StreamObserver;
import org.junit.*;

import java.net.UnknownHostException;
import java.time.Clock;
import java.time.Duration;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author Marc Gathier
 */
public class MultiTierReaderEventStoreLocatorTest {

    private static final String STORAGE_CONTEXT = "ctx1";
    private static final String NO_STORAGE_CONTEXT = "ctx2";
    public static final boolean FORCE_READ_FROM_LEADER = true;
    private MultiTierReaderEventStoreLocator testSubject;

    private LocalEventStore localEventStore = mock(LocalEventStore.class);
    private LowerTierEventStore lowerTierEventStore = mock(LowerTierEventStore.class);
    private EventStore leaderEventStore = mock(EventStore.class);


    @Before
    public void setup() {
        RaftGroupRepositoryManager raftGroupRepositoryManager = mock(RaftGroupRepositoryManager.class);
        when(raftGroupRepositoryManager.containsStorageContext(STORAGE_CONTEXT)).thenReturn(true);
        when(raftGroupRepositoryManager.containsStorageContext(NO_STORAGE_CONTEXT)).thenReturn(false);

        LowerTierEventStoreLocator lowerTierEventStoreLocator = mock(LowerTierEventStoreLocator.class);
        when(lowerTierEventStoreLocator.getEventStore(anyString())).thenReturn(lowerTierEventStore);

        LeaderEventStoreLocator leaderEventStoreLocator = mock(LeaderEventStoreLocator.class);
        when(leaderEventStoreLocator.getEventStore(anyString())).thenReturn(leaderEventStore);
        doAnswer(invocation -> {
            StreamObserver<TrackingToken> responseStream = invocation.getArgument(2);
            responseStream.onNext(TrackingToken.newBuilder().setToken(100).build());
            responseStream.onCompleted();
            return null;
        }).when(leaderEventStore).getFirstToken(anyString(), any(), any());

        when(localEventStore.firstToken(anyString())).thenReturn(50L);

        EmbeddedDBProperties embeddedDBProperties = new EmbeddedDBProperties(new SystemInfoProvider() {
            @Override
            public String getHostName() throws UnknownHostException {
                return null;
            }
        });
        embeddedDBProperties.getEvent().setRetentionTime(new Duration[]{Duration.ofMillis(1000)});
        when(lowerTierEventStore.toString()).thenReturn("LowerTierEventStore");

        when(leaderEventStore.toString()).thenReturn("LeaderEventStore");
        when(localEventStore.toString()).thenReturn("LocalEventStore");

        testSubject = new MultiTierReaderEventStoreLocator(raftGroupRepositoryManager,
                                                           lowerTierEventStoreLocator,
                                                           leaderEventStoreLocator,
                                                           embeddedDBProperties,
                                                           Clock.systemUTC(),
                                                           localEventStore);
    }

    @Test
    public void getEventStoreWithTokenFromLocalEventStore() {
        assertEquals(localEventStore, testSubject.getEventStoreWithToken(STORAGE_CONTEXT,
                                                                         !FORCE_READ_FROM_LEADER, 100));
    }

    @Test
    public void getEventStoreWithTokenFromMessagingOnlyNode() {
        assertEquals(lowerTierEventStore, testSubject.getEventStoreWithToken(NO_STORAGE_CONTEXT,
                                                                             !FORCE_READ_FROM_LEADER, 100));
    }

    @Test
    public void getEventStoreWithTokenFromLeaderEventStore() {
        assertEquals(leaderEventStore, testSubject.getEventStoreWithToken(STORAGE_CONTEXT,
                                                                          FORCE_READ_FROM_LEADER, 100));
    }

    @Test
    public void getEventStoreWithTokenFromLowerTier() {
        assertEquals(lowerTierEventStore, testSubject.getEventStoreWithToken(STORAGE_CONTEXT,
                                                                             FORCE_READ_FROM_LEADER, 10));
    }

    @Test
    public void getEventStoreFromTimestampFromLocalEventStore() {
        long timestamp = System.currentTimeMillis() - 100;
        assertEquals(localEventStore, testSubject.getEventStoreFromTimestamp(STORAGE_CONTEXT,
                                                                             !FORCE_READ_FROM_LEADER, timestamp));
    }

    @Test
    public void getEventStoreFromTimestampFromLeader() {
        long timestamp = System.currentTimeMillis() - 100;
        assertEquals(leaderEventStore, testSubject.getEventStoreFromTimestamp(STORAGE_CONTEXT,
                                                                              FORCE_READ_FROM_LEADER, timestamp));
    }

    @Test
    public void getEventStoreFromTimestampFromLowerTier() {
        long timestamp = System.currentTimeMillis() - 2000;
        assertEquals(lowerTierEventStore, testSubject.getEventStoreFromTimestamp(STORAGE_CONTEXT,
                                                                                 !FORCE_READ_FROM_LEADER,
                                                                                 timestamp));
    }

    @Test
    public void getEventStoreFromTimestampFromLowerTierNotOnLeader() {
        long timestamp = System.currentTimeMillis() - 2000;
        assertEquals(lowerTierEventStore, testSubject.getEventStoreFromTimestamp(STORAGE_CONTEXT,
                                                                                 FORCE_READ_FROM_LEADER,
                                                                                 timestamp));
    }
}