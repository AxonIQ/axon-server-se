package io.axoniq.axonhub.rest;

import io.axoniq.axonhub.cluster.ClusterController;
import io.axoniq.axonhub.cluster.jpa.ClusterNode;
import io.axoniq.axonhub.config.MessagingPlatformConfiguration;
import io.axoniq.axonhub.context.ContextController;
import io.axoniq.axonhub.grpc.internal.RemoteConnection;
import io.axoniq.axonhub.licensing.Limits;
import io.axoniq.axonhub.message.command.CommandDispatcher;
import io.axoniq.axonhub.message.event.EventDispatcher;
import io.axoniq.axonhub.message.query.QueryDispatcher;
import io.axoniq.axonhub.message.query.subscription.FakeSubscriptionMetrics;
import org.junit.*;
import org.junit.runner.*;
import org.mockito.*;
import org.mockito.runners.*;

import java.util.Collection;
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
    @Mock
    private Limits limits;
    @Mock
    private ClusterController clusterController;
    @Mock
    private CommandDispatcher commandDispatcher;
    @Mock
    private QueryDispatcher queryDispatcher;
    @Mock
    private EventDispatcher eventDispatcher;

    @Mock
    private ContextController contextController;

    @Before
    public void setup() {
        MessagingPlatformConfiguration messagePlatformConfiguration = new MessagingPlatformConfiguration(null);
        testSubject = new PublicRestController(clusterController, commandDispatcher, queryDispatcher, eventDispatcher, contextController, limits,
                                               messagePlatformConfiguration,
                                               () -> new FakeSubscriptionMetrics(500, 400, 1000));

        ClusterNode other = new ClusterNode("node2", "host2", "host2", 100, 200, 300);
        ClusterNode me = new ClusterNode("node1", "host1", "host1", 100, 200, 300);
        Collection<RemoteConnection> nodes = Collections.singleton(new RemoteConnection(me, other,
                null,
                null, messagePlatformConfiguration));
        when(clusterController.getRemoteConnections()).thenReturn(nodes);
        when(clusterController.getMe()).thenReturn(me);
        when(eventDispatcher.getNrOfEvents()).thenReturn(200L);

        when(queryDispatcher.getNrOfQueries()).thenReturn(300L);
        when(commandDispatcher.getNrOfCommands()).thenReturn(100L);
    }

    @Test
    public void getClusterNodes() {
        List<io.axoniq.axonhub.rest.ClusterNode> nodes = testSubject.getClusterNodes();
        assertEquals(2, nodes.size());
        assertEquals("node1", nodes.get(0).getName());
        assertEquals("node2", nodes.get(1).getName());
    }

    @Test
    public void getNodeInfo() {
        io.axoniq.axonhub.rest.ClusterNode node = testSubject.getNodeInfo();
        assertEquals("node1", node.getName());
        assertEquals("host1", node.getHostName());
        assertEquals("host1", node.getInternalHostName());
        assertEquals(100, node.getGrpcPort());
        assertEquals(200, node.getInternalGrpcPort());
        assertEquals(300, node.getHttpPort());
    }


    @Test
    public void licenseInfo() {
        LicenseInfo licenseInfo = testSubject.licenseInfo();
        assertEquals("Free", licenseInfo.getEdition());
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