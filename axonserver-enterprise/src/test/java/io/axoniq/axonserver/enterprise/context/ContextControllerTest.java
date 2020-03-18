package io.axoniq.axonserver.enterprise.context;

import io.axoniq.axonserver.AxonServerEnterprise;
import io.axoniq.axonserver.GrpcMonitoringProperties;
import io.axoniq.axonserver.cluster.grpc.LogReplicationService;
import io.axoniq.axonserver.enterprise.ContextEvents;
import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.enterprise.cluster.GrpcRaftController;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.enterprise.jpa.Context;
import io.axoniq.axonserver.enterprise.jpa.ContextClusterNode;
import io.axoniq.axonserver.grpc.cluster.Role;
import io.axoniq.axonserver.grpc.internal.NodeInfo;
import io.axoniq.axonserver.grpc.internal.NodeInfoWithLabel;
import io.axoniq.axonserver.topology.Topology;
import io.axoniq.axonserver.version.VersionInfoProvider;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.junit.*;
import org.junit.runner.*;
import org.mockito.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
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

    @Autowired
    private ContextRepository contextRepository;

    @MockBean
    private VersionInfoProvider versionInfoProvider;

    @Mock
    private ApplicationEventPublisher eventPublisher;

    @Mock
    private ClusterController clusterController;

    @MockBean
    private LogReplicationService logReplicationService;
    @MockBean
    private GrpcRaftController raftController;
    @MockBean
    private GrpcMonitoringProperties grpcMetricsConfig;
    @MockBean
    private PrometheusMeterRegistry prometheusMeterRegistry;

    private List<NodeInfoWithLabel> initialNodes = new ArrayList<>();

    @Before
    public void setUp()  {
        Context defaultContext = new Context(Topology.DEFAULT_CONTEXT);
        entityManager.persist(defaultContext);
        ClusterNode node1 = new ClusterNode("node1", "node1", "node1", 8124, 8224, 8024);

        ClusterNode node2 = new ClusterNode("node2", "node2", "node2", 8124, 8224, 8024);
        node1.addContext(defaultContext, "node1", Role.PRIMARY);
        node2.addContext(defaultContext, "node2", Role.PRIMARY);
        initialNodes.add(nodeInfo(node1, Role.PRIMARY));
        initialNodes.add(nodeInfo(node2, Role.PRIMARY));
        entityManager.persist(node1);
        entityManager.persist(node2);
        entityManager.flush();
        doAnswer(invocationOnMock -> {
            NodeInfo nodeInfo = invocationOnMock.getArgument(0);
            ClusterNode clusterNode = new ClusterNode(nodeInfo.getNodeName(),
                                                      nodeInfo.getHostName(),
                                                      nodeInfo.getInternalHostName(),
                                                      nodeInfo.getGrpcPort(),
                                                      nodeInfo.getGrpcInternalPort(),
                                                      nodeInfo.getHttpPort());
            entityManager.persist(clusterNode);
            return clusterNode;
        }).when(clusterController).addConnection(any(NodeInfo.class));

        doAnswer(i -> {
            String node = i.getArgument(0);
            return entityManager.find(ClusterNode.class, node);
        }).when(clusterController).getNode(anyString());

        testSubject = new ContextController(contextRepository, clusterController);
    }

    private NodeInfoWithLabel nodeInfo(ClusterNode clusterNode, Role role) {
        return NodeInfoWithLabel.newBuilder()
                                .setNode(clusterNode.toNodeInfo())
                                .setLabel(clusterNode.getName())
                                .setRole(role)
                                .build();
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
        nodes.add(nodeInfo(node3, Role.ACTIVE_BACKUP));

        testSubject.updateContext(io.axoniq.axonserver.grpc.internal.ContextConfiguration.newBuilder().setContext(Topology.DEFAULT_CONTEXT).addAllNodes(nodes).build());

        entityManager.flush();
        Context defaultContext = entityManager.find(Context.class, Topology.DEFAULT_CONTEXT);
        assertEquals(3, defaultContext.getNodes().size());
        Optional<ContextClusterNode> optionalMember = defaultContext
                .getNodes().stream().filter(c -> c.getClusterNode().getName().equals("node3")).findFirst();
        assertTrue(optionalMember.isPresent());
        assertEquals(Role.ACTIVE_BACKUP, optionalMember.get().getRole());
    }

    @Test
    public void addNewNodeToContext() {
        ClusterNode node3 = new ClusterNode("node3", "node3", "node3", 8124, 8224, 8024);
        List<NodeInfoWithLabel> nodes = new ArrayList<>(initialNodes);
        nodes.add(nodeInfo(node3, Role.ACTIVE_BACKUP));

        testSubject.updateContext(io.axoniq.axonserver.grpc.internal.ContextConfiguration.newBuilder().setContext(
                Topology.DEFAULT_CONTEXT).addAllNodes(nodes).build());

        entityManager.flush();
        Context defaultContext = entityManager.find(Context.class, Topology.DEFAULT_CONTEXT);
        assertEquals(3, defaultContext.getNodes().size());
        Optional<ContextClusterNode> optionalMember = defaultContext
                .getNodes().stream().filter(c -> c.getClusterNode().getName().equals("node3")).findFirst();
        assertTrue(optionalMember.isPresent());
        assertEquals(Role.ACTIVE_BACKUP, optionalMember.get().getRole());
    }

    @Test
    public void deleteContext() {
        // given context test1 connected to nodes 1 and 2
        Context test1 = new Context("test1");
        entityManager.persist(test1);

        entityManager.createQuery("select c from ClusterNode c", ClusterNode.class).getResultList().forEach(n -> n.addContext(test1,
                                                                                                                              n.getName(),
                                                                                                                              Role.PRIMARY));
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
    public void on() {
        testSubject.updateContext(io.axoniq.axonserver.grpc.internal.ContextConfiguration.newBuilder().setContext(
                "test1").addNodes(initialNodes.get(0)).build());
        assertEquals(2, testSubject.getContexts().count());
        testSubject.on(new ContextEvents.AdminContextDeleted("_admin"));
        assertEquals(0, testSubject.getContexts().count());
    }
}