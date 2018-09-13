package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.features.DefaultFeatureChecker;
import io.axoniq.axonserver.features.FeatureChecker;
import io.axoniq.axonserver.message.command.CommandDispatcher;
import io.axoniq.axonserver.message.event.EventDispatcher;
import io.axoniq.axonserver.message.query.QueryDispatcher;
import io.axoniq.axonserver.message.query.subscription.FakeSubscriptionMetrics;
import io.axoniq.axonserver.topology.AxonServerNode;
import io.axoniq.axonserver.topology.SimpleAxonServerNode;
import io.axoniq.axonserver.topology.Topology;
import org.junit.*;
import org.junit.runner.*;
import org.mockito.*;
import org.mockito.runners.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Author: marc
 */
@RunWith(MockitoJUnitRunner.class)
public class PublicRestControllerTest {
    private PublicRestController testSubject;
    private FeatureChecker limits = new DefaultFeatureChecker();
    @Mock
    private Topology clusterController;
    @Mock
    private CommandDispatcher commandDispatcher;
    @Mock
    private QueryDispatcher queryDispatcher;
    @Mock
    private EventDispatcher eventDispatcher;

    @Before
    public void setup() {
        MessagingPlatformConfiguration messagePlatformConfiguration = new MessagingPlatformConfiguration(null);
        testSubject = new PublicRestController(clusterController, commandDispatcher, queryDispatcher, eventDispatcher,  limits,
                                               messagePlatformConfiguration,
                                               () -> new FakeSubscriptionMetrics(500, 400, 1000));

        AxonServerNode other = new SimpleAxonServerNode("node2", "host2", 100, 200);
        AxonServerNode me = new SimpleAxonServerNode("node1", "host1", 100, 200);
        List<AxonServerNode> nodes = new ArrayList<>();
        nodes.add(other);
        when(clusterController.getRemoteConnections()).thenReturn(nodes);
        when(clusterController.getMe()).thenReturn(me);
        when(eventDispatcher.getNrOfEvents()).thenReturn(200L);

        when(queryDispatcher.getNrOfQueries()).thenReturn(300L);
        when(commandDispatcher.getNrOfCommands()).thenReturn(100L);
    }

    @Test
    public void getClusterNodes() {
        List<PublicRestController.JsonServerNode> nodes = testSubject.getClusterNodes();
        assertEquals(2, nodes.size());
        assertEquals("node1", nodes.get(0).getName());
        assertEquals("node2", nodes.get(1).getName());
    }

    @Test
    public void getNodeInfo() {
        AxonServerNode node = testSubject.getNodeInfo();
        assertEquals("node1", node.getName());
        assertEquals("host1", node.getHostName());
        assertNull( node.getInternalHostName());
        assertEquals(Integer.valueOf(100), node.getGrpcPort());
        assertEquals(Integer.valueOf(200), node.getHttpPort());
    }


    @Test
    public void licenseInfo() {
        LicenseInfo licenseInfo = testSubject.licenseInfo();
        assertEquals("Free edition", licenseInfo.getEdition());
    }


    @Test
    public void status() {
        StatusInfo status = testSubject.status();
        assertEquals(100, status.getNrOfCommands());
        assertEquals(200, status.getNrOfEvents());
        assertEquals(300, status.getNrOfQueries());
        assertEquals(500, status.getNrOfSubscriptionQueries());
        assertEquals(400, status.getNrOfActiveSubscriptionQueries());
        assertEquals(1000, status.getNrOfSubscriptionQueriesUpdates());
    }
}