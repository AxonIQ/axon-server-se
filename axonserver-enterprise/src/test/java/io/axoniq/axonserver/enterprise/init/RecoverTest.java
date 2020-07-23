package io.axoniq.axonserver.enterprise.init;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.axoniq.axonserver.cluster.jpa.ReplicationGroupMember;
import io.axoniq.axonserver.cluster.jpa.ReplicationGroupMemberRepository;
import io.axoniq.axonserver.enterprise.jpa.ClusterNodeRepository;
import io.axoniq.axonserver.enterprise.jpa.AdminReplicationGroup;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.cluster.Role;
import org.junit.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.persistence.EntityManager;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author Marc Gathier
 */
public class RecoverTest {

    private Recover testSubject;
    private File recoveryFile;
    private Map<String, ClusterNode> clusterNodeMap = new HashMap<>();
    private Map<String, ReplicationGroupMember> raftGroupNodeMap = new HashMap<>();

    @Before
    public void setup() throws IOException {
        recoveryFile = File.createTempFile("recovery", ".json");

        ClusterNodeRepository clusterNodeRepository = mock(ClusterNodeRepository.class);

        doAnswer(invocation -> {
            String id = invocation.getArgument(0);
            return Optional.ofNullable(clusterNodeMap.get(id));
        }).when(clusterNodeRepository).findById(anyString());

        doAnswer(invocation -> {
            ClusterNode clusterNode = invocation.getArgument(0);
            clusterNodeMap.remove(clusterNode.getName());
            return null;
        }).when(clusterNodeRepository).delete(any(ClusterNode.class));

        doAnswer(invocation -> {
            ClusterNode clusterNode = invocation.getArgument(0);
            clusterNodeMap.put(clusterNode.getName(), clusterNode);
            return null;
        }).when(clusterNodeRepository).save(any(ClusterNode.class));

        doAnswer(invocation -> {
            ClusterNode clusterNode = invocation.getArgument(0);
            clusterNodeMap.put(clusterNode.getName(), clusterNode);
            return null;
        }).when(clusterNodeRepository).saveAndFlush(any(ClusterNode.class));

        ReplicationGroupMemberRepository jpaRaftGroupNodeRepository = mock(ReplicationGroupMemberRepository.class);

        doAnswer(invocation -> {
            String id = invocation.getArgument(0);
            return raftGroupNodeMap.values().stream().filter(n -> n.getNodeName().endsWith(id)).collect(Collectors
                                                                                                                .toSet());
        }).when(jpaRaftGroupNodeRepository).findByNodeName(anyString());

        doAnswer(invocation -> {
            Collection<ReplicationGroupMember> updatedNodes = invocation.getArgument(0);
            updatedNodes.forEach(node -> raftGroupNodeMap.put(key(node), node));
            return null;
        }).when(jpaRaftGroupNodeRepository).saveAll(anyCollection());


        testSubject = new Recover(mock(EntityManager.class),
                                  clusterNodeRepository,
                                  jpaRaftGroupNodeRepository,
                                  recoveryFile.getAbsolutePath());

        AdminReplicationGroup context = new AdminReplicationGroup();
        context.setName("sampleContext");
        ClusterNode clusterNode1 = new ClusterNode("node1", "hostname1", "internalHostName1", 1, 2, 3);
        ClusterNode clusterNode2 = new ClusterNode("node2", "hostname2", "internalHostName2", 1, 2, 3);
        ClusterNode clusterNode3 = new ClusterNode("node3", "hostname3", "internalHostName3", 1, 2, 3);

        clusterNode1.addReplicationGroup(context, "node1label", Role.PRIMARY);
        clusterNode2.addReplicationGroup(context, "node2label", Role.PRIMARY);
        clusterNode3.addReplicationGroup(context, "node3label", Role.PRIMARY);

        clusterNodeMap.put(clusterNode1.getName(), clusterNode1);
        clusterNodeMap.put(clusterNode2.getName(), clusterNode2);
        clusterNodeMap.put(clusterNode3.getName(), clusterNode3);

        ReplicationGroupMember raftMember1 = new ReplicationGroupMember("sampleContext",
                                                                        Node.newBuilder()
                                                                            .setRole(Role.PRIMARY)
                                                                            .setNodeName("node1")
                                                                            .setNodeId("node1label")
                                                                            .setPort(2)
                                                                            .setHost("hostname1")
                                                                            .build());
        ReplicationGroupMember raftMember2 = new ReplicationGroupMember("sampleContext",
                                                                        Node.newBuilder()
                                                                            .setRole(Role.PRIMARY)
                                                                            .setNodeName("node2")
                                                                            .setNodeId("node2label")
                                                                            .setPort(2)
                                                                            .setHost("hostname2")
                                                                            .build());
        ReplicationGroupMember raftMember3 = new ReplicationGroupMember("sampleContext",
                                                                        Node.newBuilder()
                                                                            .setRole(Role.PRIMARY)
                                                                            .setNodeName("node3")
                                                                            .setNodeId("node3label")
                                                                            .setPort(2)
                                                                            .setHost("hostname3")
                                                                            .build());

        raftGroupNodeMap.put(key(raftMember1), raftMember1);
        raftGroupNodeMap.put(key(raftMember2), raftMember2);
        raftGroupNodeMap.put(key(raftMember3), raftMember3);
    }

    private String key(ReplicationGroupMember raftMember) {
        return raftMember.getGroupId() + "/" + raftMember.getNodeId();
    }

    @After
    public void cleanupTempFile() {
        if (recoveryFile != null) {
            recoveryFile.delete();
        }
    }

    @Test
    public void changeNodeNames() throws IOException {
        ObjectMapper mapper = new ObjectMapper();

        RecoverNode node1 = new RecoverNode();
        node1.setOldName("node1");
        node1.setName("newNode1");
        RecoverNode node2 = new RecoverNode();
        node2.setOldName("node2");
        node2.setName("newNode2");
        RecoverNode node3 = new RecoverNode();
        node3.setOldName("node3");
        node3.setName("newNode3");
        List<RecoverNode> nodes = new ArrayList<>(Arrays.asList(node1, node2, node3));

        mapper.writeValue(recoveryFile, nodes);

        testSubject.start();

        assertEquals(3, clusterNodeMap.size());
        assertNotNull(clusterNodeMap.get("newNode1"));
        assertNotNull(clusterNodeMap.get("newNode2"));
        assertNotNull(clusterNodeMap.get("newNode3"));
        assertNull(clusterNodeMap.get("node3"));

        ClusterNode newNode1 = clusterNodeMap.get("newNode1");
        assertEquals("hostname1", newNode1.getHostName());
        assertEquals("internalHostName1", newNode1.getInternalHostName());
        assertEquals(1, newNode1.getReplicationGroups().size());

        assertEquals(3, raftGroupNodeMap.size());

        ReplicationGroupMember group1 = raftGroupNodeMap.get("sampleContext/node1label");
        assertNotNull(group1);
        assertEquals("newNode1", group1.getNodeName());
        ReplicationGroupMember group2 = raftGroupNodeMap.get("sampleContext/node2label");
        assertNotNull(group2);
        assertEquals("newNode2", group2.getNodeName());
        ReplicationGroupMember group3 = raftGroupNodeMap.get("sampleContext/node3label");
        assertNotNull(group3);
        assertEquals("newNode3", group3.getNodeName());
    }

    @Test
    public void changeHostNames() throws IOException {
        ObjectMapper mapper = new ObjectMapper();

        RecoverNode node1 = new RecoverNode();
        node1.setName("node1");
        node1.setHostName("newHostname1");
        node1.setGrpcPort(4);
        node1.setInternalGrpcPort(5);
        node1.setHttpPort(6);
        node1.setInternalHostName("newInternalHostname1");
        RecoverNode node2 = new RecoverNode();
        node2.setName("node2");
        node2.setInternalHostName("newInternalHostname2");
        RecoverNode node3 = new RecoverNode();
        node3.setName("node3");
        node3.setInternalHostName("newInternalHostname3");
        List<RecoverNode> nodes = new ArrayList<>(Arrays.asList(node1, node2, node3));

        mapper.writeValue(recoveryFile, nodes);

        testSubject.start();

        assertEquals(3, clusterNodeMap.size());
        assertNotNull(clusterNodeMap.get("node1"));
        assertNotNull(clusterNodeMap.get("node2"));
        assertNotNull(clusterNodeMap.get("node3"));

        ClusterNode newNode1 = clusterNodeMap.get("node1");
        assertEquals("newHostname1", newNode1.getHostName());
        assertEquals("newInternalHostname1", newNode1.getInternalHostName());
        assertEquals(4, (long) newNode1.getGrpcPort());
        assertEquals(5, (long) newNode1.getGrpcInternalPort());
        assertEquals(6, (long) newNode1.getHttpPort());
        assertEquals(1, newNode1.getReplicationGroups().size());

        assertEquals(3, raftGroupNodeMap.size());

        ReplicationGroupMember group = raftGroupNodeMap.get("sampleContext/node1label");
        assertNotNull(group);
        assertEquals("node1", group.getNodeName());
        assertEquals("newInternalHostname1", group.getHost());
        assertEquals(5, group.getPort());
    }
}