package io.axoniq.axonserver.enterprise.replication.admin;

import io.axoniq.axonserver.AxonServer;
import io.axoniq.axonserver.access.application.AdminApplicationController;
import io.axoniq.axonserver.access.user.UserController;
import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import io.axoniq.axonserver.enterprise.component.processor.balancing.stategy.AdminProcessorLoadBalancingService;
import io.axoniq.axonserver.enterprise.jpa.AdminContextRepository;
import io.axoniq.axonserver.enterprise.jpa.AdminReplicationGroup;
import io.axoniq.axonserver.enterprise.jpa.AdminReplicationGroupMember;
import io.axoniq.axonserver.enterprise.jpa.AdminReplicationGroupRepository;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.enterprise.jpa.ClusterNodeRepository;
import io.axoniq.axonserver.grpc.internal.Context;
import io.axoniq.axonserver.grpc.internal.NodeInfo;
import io.axoniq.axonserver.grpc.internal.NodeInfoWithLabel;
import io.axoniq.axonserver.grpc.internal.ReplicationGroupConfiguration;
import io.axoniq.axonserver.grpc.internal.ReplicationGroupContexts;
import io.axoniq.axonserver.version.VersionInfoProvider;
import org.junit.*;
import org.junit.runner.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author Marc Gathier
 */
@RunWith(SpringRunner.class)
@DataJpaTest
@ComponentScan(basePackages = {
        "io.axoniq.axonserver.enterprise.replication.admin",
        "io.axoniq.axonserver.enterprise.jpa",
}, lazyInit = true)
@ContextConfiguration(classes = AxonServer.class)
public class AdminReplicationGroupControllerTest {

    private AdminReplicationGroupController testSubject;
    @Autowired
    private AdminReplicationGroupRepository replicationGroupRepository;
    @Autowired
    private AdminContextRepository contextRepository;
    @Autowired
    private ClusterNodeRepository clusterNodeRepository;
    @MockBean
    private AdminApplicationController applicationController;
    @MockBean
    private UserController userController;
    @MockBean
    private ClusterController clusterController;
    @MockBean
    private AdminProcessorLoadBalancingService processorLoadBalancingService;
    @MockBean
    private ApplicationEventPublisher applicationEventPublisher;
    @MockBean
    private VersionInfoProvider versionInfoProvider;

    @Before
    public void setup() {
        testSubject = new AdminReplicationGroupController(replicationGroupRepository, contextRepository,
                                                          applicationController, userController, clusterController,
                                                          processorLoadBalancingService, applicationEventPublisher);
        when(clusterController.addConnection(any())).then(invocation -> {
            ClusterNode c = ClusterNode.from(invocation.getArgument(0));
            return clusterNodeRepository.save(c);
        });
    }

    @Test
    public void updateReplicationGroup() {
        testSubject.updateReplicationGroup(ReplicationGroupConfiguration.newBuilder()
                                                                        .setReplicationGroupName("Test1")
                                                                        .addNodes(NodeInfoWithLabel.newBuilder()
                                                                                                   .setLabel(
                                                                                                           "Node1Label")
                                                                                                   .setNode(NodeInfo.newBuilder()
                                                                                                                    .setNodeName(
                                                                                                                            "Node1Name")
                                                                                                                    .setInternalHostName(
                                                                                                                            "InternalHostName")
                                                                                                                    .setGrpcInternalPort(
                                                                                                                            1234)
                                                                                                                    .build())
                                                                                                   .build())
                                                                        .build());
        Optional<AdminReplicationGroup> replicationGroup = replicationGroupRepository
                .findByName("Test1");
        assertTrue(replicationGroup.isPresent());
        assertEquals(1, replicationGroup.get().getMembers().size());
        testSubject.updateReplicationGroup(ReplicationGroupConfiguration.newBuilder()
                                                                        .setReplicationGroupName("Test1")
                                                                        .addNodes(NodeInfoWithLabel.newBuilder()
                                                                                                   .setLabel(
                                                                                                           "Node2Label")
                                                                                                   .setNode(NodeInfo.newBuilder()
                                                                                                                    .setNodeName(
                                                                                                                            "Node2Name")
                                                                                                                    .setInternalHostName(
                                                                                                                            "InternalHostName2")
                                                                                                                    .setGrpcInternalPort(
                                                                                                                            1234)
                                                                                                                    .build())
                                                                                                   .build())
                                                                        .build());
        replicationGroup = replicationGroupRepository
                .findByName("Test1");
        assertTrue(replicationGroup.isPresent());
        assertEquals(1, replicationGroup.get().getMembers().size());
    }

    @Test
    public void onPrepareForDeleteNode() {
        createReplicationGroup("name", "node");
        AdminReplicationGroupMember member;

        ClusterEvents.DeleteNodeFromReplicationGroupRequested replicationGroupDeleted =
                new ClusterEvents.DeleteNodeFromReplicationGroupRequested("name", "node");
        testSubject.on(replicationGroupDeleted);
        Optional<AdminReplicationGroup> optionalReplicationGroup = replicationGroupRepository
                .findByName("name");
        assertTrue(optionalReplicationGroup.isPresent());
        assertEquals(1, optionalReplicationGroup.get().getMembers().size());
        member = optionalReplicationGroup.get().getMembers().iterator().next();
        assertTrue(member.isPendingDelete());
    }

    private void createReplicationGroup(String replicationGroupName, String... nodes) {
        AdminReplicationGroup replicationGroup = new AdminReplicationGroup(replicationGroupName);
        for (String node : nodes) {
            ClusterNode clusterNode = new ClusterNode(node, node, node, 1, 2, 3);
            clusterNodeRepository.save(clusterNode);
            clusterNodeRepository.flush();
            AdminReplicationGroupMember member = new AdminReplicationGroupMember();
            member.setClusterNode(clusterNode);
            replicationGroup.addMember(member);
        }
        replicationGroupRepository.save(replicationGroup);
        replicationGroupRepository.flush();
    }

    @Test
    public void testOn() {
        ClusterEvents.ReplicationGroupDeleted replicationGroupDeleted = new ClusterEvents.ReplicationGroupDeleted(
                "_admin",
                true);
        testSubject.on(replicationGroupDeleted);
        verify(applicationController).clearApplications();
        verify(userController).deleteAll();
        verify(processorLoadBalancingService).deleteAll();
    }

    @Test
    public void getNodeNames() {
        createReplicationGroup("rg1", "node1", "node2", "node3");
        Collection<String> names = testSubject.getNodeNames("rg1");
        assertEquals(3, names.size());
    }

    @Test
    public void registerContext() {
        createReplicationGroup("rg2", "node1", "node2", "node3");
        testSubject.registerContext("rg2", "context1", Collections.emptyMap());
        Set<String> contexts = testSubject.contextsPerReplicationGroup("rg2");
        assertTrue(contexts.contains("context1"));
    }

    @Test
    public void registerContexts() {
        createReplicationGroup("rg3", "node1", "node2", "node3");
        testSubject.registerContexts(ReplicationGroupContexts.newBuilder()
                                                             .setReplicationGroupName("rg3")
                                                             .addContext(Context.newBuilder()
                                                                                .setContextName("demo")
                                                                                .putMetaData("key", "value")
                                                                                .build())
                                                             .build());
        Set<String> contexts = testSubject.contextsPerReplicationGroup("rg3");
        assertTrue(contexts.contains("demo"));
        testSubject.unregisterContext("rg3", "demo");
        contexts = testSubject.contextsPerReplicationGroup("rg3");
        assertFalse(contexts.contains("demo"));
    }

    @Test
    public void findByName() {
        createReplicationGroup("rg2", "node1", "node2", "node3");
        Optional<AdminReplicationGroup> optionalReplicationGroup = testSubject.findByName("rg2");
        assertTrue(optionalReplicationGroup.isPresent());
    }

    @Test
    public void testUpdateReplicationGroup() {
        testSubject.updateReplicationGroup(io.axoniq.axonserver.grpc.internal.ContextConfiguration.newBuilder()
                                                                                                  .setContext("demo")
                                                                                                  .addNodes(
                                                                                                          nodeInfoWithLabel(
                                                                                                                  "node1"))
                                                                                                  .build());
        Set<String> contexts = testSubject.contextsPerReplicationGroup("demo");
        assertTrue(contexts.contains("demo"));
    }

    private NodeInfoWithLabel nodeInfoWithLabel(String node1) {
        return NodeInfoWithLabel.newBuilder()
                                .setLabel(node1)
                                .setNode(NodeInfo.newBuilder()
                                                 .setNodeName(node1)
                                                 .setInternalHostName(node1)
                                                 .setGrpcInternalPort(1234)
                                                 .build())
                                .build();
    }

    @Test
    public void registerReplicationGroup() {
        ClusterNode clusterNode = new ClusterNode("test123", "test123", "test123", 1, 2, 3);
        clusterNode = clusterNodeRepository.save(clusterNode);
        clusterNodeRepository.flush();
        when(clusterController.getNode("test123")).thenReturn(clusterNode);
        testSubject.registerReplicationGroup(ReplicationGroupConfiguration.newBuilder()
                                                                          .setReplicationGroupName("test123")
                                                                          .addNodes(nodeInfoWithLabel("test123"))
                                                                          .build());
        Optional<AdminReplicationGroup> replicationGroup = replicationGroupRepository.findByName("test123");
        assertTrue(replicationGroup.isPresent());
        assertEquals(1, replicationGroup.get().getMembers().size());
    }
}