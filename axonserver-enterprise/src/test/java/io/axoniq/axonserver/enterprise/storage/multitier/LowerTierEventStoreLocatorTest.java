package io.axoniq.axonserver.enterprise.storage.multitier;

import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.enterprise.messaging.event.RemoteLowerTierEventStore;
import io.axoniq.axonserver.enterprise.replication.RaftGroupRepositoryManager;
import io.axoniq.axonserver.message.event.EventStore;
import io.axoniq.axonserver.metric.BaseMetricName;
import io.axoniq.axonserver.metric.MeterFactory;
import io.micrometer.core.instrument.Tags;
import org.junit.*;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author Marc Gathier
 */
public class LowerTierEventStoreLocatorTest {

    private LowerTierEventStoreLocator testSubject;
    private EventStore remoteEventStore = mock(RemoteLowerTierEventStore.class);
    private AtomicReference<String> eventStore = new AtomicReference<>();

    @Before
    public void setUp() {
        FakeMetricCollector metricCollector = new FakeMetricCollector();
        LowerTierEventStoreFactory remoteEventStoreFactory = mock(LowerTierEventStoreFactory.class);
        when(remoteEventStoreFactory.create(anyString())).then(invocation -> {
            eventStore.set(invocation.getArgument(0));
            return remoteEventStore;
        });
        MessagingPlatformConfiguration messagingPlatformConfiguration = mock(MessagingPlatformConfiguration.class);
        when(messagingPlatformConfiguration.getName()).thenReturn("me");

        RaftGroupRepositoryManager raftGroupRepositoryManager = mock(RaftGroupRepositoryManager.class);
        Set<String> nodes = new HashSet<>();
        nodes.add("NODE1");
        nodes.add("NODE2");
        nodes.add("NODE3");
        when(raftGroupRepositoryManager.nextTierEventStores(anyString())).thenReturn(nodes);

        metricCollector.gauge(BaseMetricName.AXON_EVENT_LAST_TOKEN.metric(),
                              Tags.of(MeterFactory.CONTEXT, "DEMO", "axonserver", "NODE1"),
                              100);
        metricCollector.gauge(BaseMetricName.AXON_EVENT_LAST_TOKEN.metric(),
                              Tags.of(MeterFactory.CONTEXT, "DEMO", "axonserver", "NODE2"),
                              200);
        ClusterController clusterController = mock(ClusterController.class);
        when(clusterController.isActive("NODE1")).thenReturn(true);
        when(clusterController.isActive("NODE2")).thenReturn(true);
        testSubject = new LowerTierEventStoreLocator(raftGroupRepositoryManager,
                                                     metricCollector,
                                                     remoteEventStoreFactory, clusterController);
    }

    @Test
    public void getEventStore() {
        assertEquals(remoteEventStore, testSubject.getEventStore("DEMO"));
        assertEquals("NODE2", eventStore.get());
    }

    @Test
    public void hasLowerTier() {
        assertTrue(testSubject.hasLowerTier("DEMO"));
    }
}