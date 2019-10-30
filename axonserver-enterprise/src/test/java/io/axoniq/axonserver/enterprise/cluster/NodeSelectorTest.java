package io.axoniq.axonserver.enterprise.cluster;


import io.axoniq.axonserver.cluster.jpa.JpaRaftGroupNode;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.enterprise.jpa.Context;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.cluster.Role;
import io.axoniq.axonserver.message.ClientIdentification;
import org.junit.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static io.axoniq.axonserver.RaftAdminGroup.getAdmin;
import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class NodeSelectorTest {

    private NodeSelector testSubject;
    private ClusterNode me = new ClusterNode("me", "myHost", null, null, null, null);
    private Map<String, Context> contextMap;
    private Set<String> connectedAxonServerNodes = new HashSet<>();

    @Before
    public void setUp() {
        ClusterNode secondNode = new ClusterNode("aSecondNode", "secondHost", null, null, null, null);
        Context admin = new Context(getAdmin());
        Context firstContext = new Context("first");

        me.addContext(admin, "Admin", Role.PRIMARY);
        me.addContext(firstContext, "First", Role.PRIMARY);

        Context secondContext = new Context("second");
        me.addContext(firstContext, "Second", Role.ACTIVE_BACKUP);
        secondNode.addContext(firstContext, "First", Role.PRIMARY);
        secondNode.addContext(secondContext, "Second", Role.PRIMARY);

        contextMap = new HashMap<>();
        contextMap.put(firstContext.getName(), firstContext);
        contextMap.put(secondContext.getName(), secondContext);
        contextMap.put(admin.getName(), admin);
        Map<String, ClusterNode> clusterMap = new HashMap<>();
        clusterMap.put(me.getName(), me);
        clusterMap.put(secondNode.getName(), secondNode);

        testSubject = new NodeSelector(me.getName(), new NodeSelectionStrategy() {
            @Override
            public String selectNode(ClientIdentification clientName, String componentName,
                                     Collection<String> activeNodes) {
                List<String> nodeList = new ArrayList<>(activeNodes);
                Collections.sort(nodeList);
                return nodeList.get(0);
            }

            @Override
            public boolean canRebalance(ClientIdentification clientName, String componentName,
                                        List<String> activeNodes) {
                return !me.getName().equals(selectNode(clientName, componentName, activeNodes));
            }
        }, clusterMap::get, contextMap::get, this::asRaftGroups, connectedAxonServerNodes);
    }

    private Set<JpaRaftGroupNode> asRaftGroups(String context) {
        return contextMap.get(context).getNodes().stream().map(c -> new JpaRaftGroupNode(context, Node.newBuilder()
                                                                                                      .setRole(c.getRole())
                                                                                                      .setNodeId(c.getClusterNodeLabel())
                                                                                                      .setNodeName(c.getClusterNode()
                                                                                                                    .getName())
                                                                                                      .build()))
                         .collect(
                                 Collectors.toSet());
    }

    @Test
    public void findNodeForClient() {
        assertEquals("me", testSubject.findNodeForClient("myClient", "myApplication", "first").getName());
    }

    @Test
    public void findNodeForClientNotAdmin() {
        me.removeContext(getAdmin());
        assertEquals("me", testSubject.findNodeForClient("myClient", "myApplication", "first").getName());
    }

    @Test
    public void findNodeForClientCurrentIsBackupNode() {
        connectedAxonServerNodes.add("aSecondNode");
        assertEquals("aSecondNode", testSubject.findNodeForClient("myClient", "myApplication", "second").getName());
    }

    @Test
    public void findNodeForClientNoActivePrimary() {
        try {
            testSubject.findNodeForClient("myClient", "myApplication", "second");
            fail("Should not find node");
        } catch (MessagingPlatformException ex) {
            assertEquals(ErrorCode.NO_AXONSERVER_FOR_CONTEXT, ex.getErrorCode());
        }
    }

    @Test
    public void canRebalance() {
        connectedAxonServerNodes.add("aSecondNode");
        assertTrue(testSubject.canRebalance("myClient", "myApplication", "first"));
    }
}