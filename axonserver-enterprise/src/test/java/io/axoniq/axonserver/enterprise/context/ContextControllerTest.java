package io.axoniq.axonserver.enterprise.context;

import io.axoniq.axonserver.AxonServerEnterprise;
import io.axoniq.axonserver.cluster.grpc.LogReplicationService;
import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.enterprise.cluster.GrpcRaftController;
import io.axoniq.axonserver.enterprise.cluster.internal.RemoteConnection;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.enterprise.jpa.Context;
import io.axoniq.axonserver.grpc.internal.NodeInfo;
import io.axoniq.axonserver.grpc.internal.NodeInfoWithLabel;
import io.axoniq.axonserver.topology.Topology;
import org.junit.*;
import org.junit.runner.*;
import org.mockito.*;
import org.mockito.stubbing.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import javax.persistence.EntityManager;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author Marc Gathier
 */
@RunWith(SpringRunner.class)
@DataJpaTest
@ComponentScan(basePackages = "io.axoniq.axonserver.enterprise.context", lazyInit = true)
@ContextConfiguration(classes = AxonServerEnterprise.class)
public class ContextControllerTest {

    private ContextController testSubject;

    @Autowired
    private EntityManager entityManager;

    @Mock
    private ApplicationEventPublisher eventPublisher;

    @Mock
    private ClusterController clusterController;

    @MockBean
    private LogReplicationService logReplicationService;
    @MockBean
    private GrpcRaftController raftController;

    private List<NodeInfoWithLabel> initialNodes = new ArrayList<>();

    @Before
    public void setUp()  {
        Context defaultContext = new Context(Topology.DEFAULT_CONTEXT);
        entityManager.persist(defaultContext);
        ClusterNode node1 = new ClusterNode("node1", "node1", "node1", 8124, 8224, 8024);

        ClusterNode node2 = new ClusterNode("node2", "node2", "node2", 8124, 8224, 8024);
        node1.addContext(defaultContext, "node1", true, true);
        node2.addContext(defaultContext, "node2", true, true);
        initialNodes.add(nodeInfo(node1));
        initialNodes.add(nodeInfo(node2));
        entityManager.persist(node1);
        entityManager.persist(node2);
        entityManager.flush();
        testSubject = new ContextController(entityManager, clusterController);
    }

    private NodeInfoWithLabel nodeInfo(ClusterNode clusterNode) {
        return NodeInfoWithLabel.newBuilder().setNode(clusterNode.toNodeInfo()).setLabel(clusterNode.getName()).build();
    }

    @Test
    public void getContexts() {
        List<Context> contexts = testSubject.getContexts().collect(Collectors
                                                                           .toList());
        assertEquals(1, contexts.size());
    }

    @Test
    public void addNodeToContext() {
        ClusterNode node3 = new ClusterNode("node3", "node3", "node3", 8124, 8224, 8024);
        entityManager.persist(node3);
        entityManager.flush();

        List<NodeInfoWithLabel> nodes = new ArrayList<>(initialNodes);
        nodes.add(nodeInfo(node3));

        testSubject.updateContext(io.axoniq.axonserver.grpc.internal.ContextConfiguration.newBuilder().setContext(Topology.DEFAULT_CONTEXT).addAllNodes(nodes).build());

        entityManager.flush();
        Context defaultContext = entityManager.find(Context.class, Topology.DEFAULT_CONTEXT);
        assertEquals(3, defaultContext.getNodes().size());
    }

    @Test
    public void deleteContext() {
        // given context test1 connected to nodes 1 and 2
        Context test1 = new Context("test1");
        entityManager.persist(test1);

        entityManager.createQuery("select c from ClusterNode c", ClusterNode.class).getResultList().forEach(n -> n.addContext(test1,
                                                                                                                              n.getName(), true, true));
        // when delete context test1
        testSubject.deleteContext("test1");
        // expect nodes 1 and node 2 no longer contain context text1
        entityManager.createQuery("select c from ClusterNode c", ClusterNode.class).getResultList().forEach(n -> assertFalse(n.getContextNames().contains("test1")));
    }

    @Test
    public void deleteNodeFromContext() {
        List<NodeInfoWithLabel> nodes = initialNodes.stream().filter(n -> !n.getNode().getNodeName().equals("node1")).collect(Collectors.toList());
        testSubject.updateContext(io.axoniq.axonserver.grpc.internal.ContextConfiguration.newBuilder()
                                                                                         .setContext(Topology.DEFAULT_CONTEXT)
                                                                                         .addAllNodes(nodes).build());
        ClusterNode node1 = entityManager.find(ClusterNode.class, "node1");
        assertEquals(0, node1.getContextNames().size());
    }

    @Test
    public void addContext() {
        testSubject.updateContext(io.axoniq.axonserver.grpc.internal.ContextConfiguration.newBuilder().setContext("test1").addNodes(initialNodes.get(0)).build());
        ClusterNode node1 = entityManager.find(ClusterNode.class, "node1");
        assertEquals(2, node1.getContextNames().size());
    }

    @Test
    public void update() {
    }

    @Test
    public void on() {
        RemoteConnection remoteConnection = mock(RemoteConnection.class);
        ClusterNode node2 = new ClusterNode("node2", null, null, null, null, null);
        when(remoteConnection.getClusterNode()).thenReturn(node2);

//        ClusterEvents.AxonServerInstanceConnected axonhubInstanceConnected = new ClusterEvents.AxonServerInstanceConnected(remoteConnection, 10, Collections.emptyList(),
//                                                                                                                           Collections
//                                                                                                                             .singletonList(
//                                                                                                                                     ContextRole.newBuilder().setName("test1").build()), Collections.emptyList());
//        testSubject.on(axonhubInstanceConnected);
    }
}
