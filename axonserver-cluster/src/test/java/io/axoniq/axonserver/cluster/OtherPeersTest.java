package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.configuration.current.DefaultCurrentConfiguration;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.cluster.Role;
import org.junit.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class OtherPeersTest {

    @Test
    public void iterator() {
        List<Node> nodes = Arrays.asList(node("node1", Role.PRIMARY),
                                         node("node2", Role.PRIMARY),
                                         node("node3", Role.ACTIVE_BACKUP));
        CurrentConfiguration configuration = new DefaultCurrentConfiguration(() -> nodes, Collections.emptyList());
        OtherPeers testSubject = new OtherPeers(() -> "node1",
                                                configuration,
                                                n -> new FakeRaftPeer(n.getNodeId(), n.getRole()),
                                                n -> n.getRole().equals(
                                                        Role.PRIMARY));

        List<RaftPeer> otherPeers = new ArrayList<>();
        testSubject.iterator().forEachRemaining(otherPeers::add);
        assertEquals(1, otherPeers.size());
        assertEquals("node2", otherPeers.get(0).nodeId());
    }

    private Node node(String name, Role role) {
        return Node.newBuilder().setNodeId(name).setRole(role).build();
    }
}