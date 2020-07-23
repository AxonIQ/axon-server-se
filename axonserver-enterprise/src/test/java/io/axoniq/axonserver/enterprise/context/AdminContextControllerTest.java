package io.axoniq.axonserver.enterprise.context;

import io.axoniq.axonserver.AxonServerEnterprise;
import io.axoniq.axonserver.GrpcMonitoringProperties;
import io.axoniq.axonserver.cluster.grpc.LogReplicationService;
import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.enterprise.jpa.AdminContextRepository;
import io.axoniq.axonserver.enterprise.jpa.AdminReplicationGroup;
import io.axoniq.axonserver.enterprise.jpa.AdminReplicationGroupMember;
import io.axoniq.axonserver.enterprise.replication.GrpcRaftController;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.enterprise.jpa.AdminContext;
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
import java.util.Set;
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
public class AdminContextControllerTest {

    private AdminContextController testSubject;

    @Autowired
    private EntityManager entityManager;

    @Autowired
    private AdminContextRepository contextRepository;

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
        AdminContext defaultContext = new AdminContext(Topology.DEFAULT_CONTEXT);
        entityManager.persist(defaultContext);
        ClusterNode node1 = new ClusterNode("node1", "node1", "node1", 8124, 8224, 8024);

        ClusterNode node2 = new ClusterNode("node2", "node2", "node2", 8124, 8224, 8024);
//        node1.addContext(defaultContext, "node1", Role.PRIMARY);
//        node2.addContext(defaultContext, "node2", Role.PRIMARY);
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

        testSubject = new AdminContextController(contextRepository);
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
        List<AdminContext> contexts = testSubject.getContexts().collect(Collectors
                                                                                .toList());
        assertEquals(1, contexts.size());
    }

    @Test
    public void findConnectableNodes() {
        AdminContext adminContext = new AdminContext("demo1");
        AdminReplicationGroup replicationGroup = new AdminReplicationGroup("replicationGroup1");
        ClusterNode node1 = clusterNode("xnode1");
        ClusterNode node2 = clusterNode("xnode2");
        ClusterNode node3 = clusterNode("xnode3");
        ClusterNode node4 = clusterNode("xnode4");
        ClusterNode node5 = clusterNode("xnode5");
        entityManager.persist(node1);
        entityManager.persist(node2);
        entityManager.persist(node3);
        entityManager.persist(node4);
        entityManager.persist(node5);

        replicationGroup.addMember(adminReplicationGroupMember(node1, Role.PRIMARY));
        replicationGroup.addMember(adminReplicationGroupMember(node2, Role.SECONDARY));
        replicationGroup.addMember(adminReplicationGroupMember(node3, Role.ACTIVE_BACKUP));
        replicationGroup.addMember(adminReplicationGroupMember(node4, Role.PASSIVE_BACKUP));
        replicationGroup.addMember(adminReplicationGroupMember(node5, Role.MESSAGING_ONLY));
        replicationGroup.addContext(adminContext);
        entityManager.persist(replicationGroup);

        Set<String> nodes = testSubject.findConnectableNodes("demo1");
        assertEquals(2, nodes.size());
        assertTrue(nodes.contains("xnode1"));
        assertTrue(nodes.contains("xnode5"));
    }

    private AdminReplicationGroupMember adminReplicationGroupMember(ClusterNode node, Role role) {
        AdminReplicationGroupMember member = new AdminReplicationGroupMember();
        member.setClusterNode(node);
        member.setClusterNodeLabel(node.getName());
        member.setRole(role);
        return member;
    }

    private ClusterNode clusterNode(String node1) {
        return new ClusterNode(node1, node1, node1, 1, 2, 3);
    }
}
