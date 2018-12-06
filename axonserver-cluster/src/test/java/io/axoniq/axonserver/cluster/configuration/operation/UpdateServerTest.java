package io.axoniq.axonserver.cluster.configuration.operation;

import io.axoniq.axonserver.grpc.cluster.Node;
import org.junit.*;

import java.util.Collections;
import java.util.List;

import static io.axoniq.axonserver.grpc.cluster.Node.newBuilder;
import static java.util.Arrays.asList;
import static org.junit.Assert.*;

/**
 * @author Sara Pellegrini
 */
public class UpdateServerTest {

    @Test(expected = IllegalArgumentException.class)
    public void updateNodeToEmptyList() {
        Node node = Node.newBuilder().build();
        UpdateServer updateServer = new UpdateServer(node);
        updateServer.apply(Collections.emptyList());
    }

    @Test
    public void updateNode() {
        Node updateNode = newBuilder().setNodeId("myNode").setHost("newHost").build();
        UpdateServer updateServer = new UpdateServer(updateNode);
        Node initialNode = newBuilder().setNodeId("myNode").setHost("localhost").build();
        Node otherNode = newBuilder().setNodeId("otherNode").build();
        List<Node> nodes = asList(initialNode, otherNode);
        List<Node> result = updateServer.apply(nodes);
        assertEquals(2, result.size());
        assertTrue(result.contains(updateNode));
        assertTrue(result.contains(otherNode));
        assertFalse(result.contains(initialNode));
    }

    @Test(expected = IllegalArgumentException.class)
    public void updateMissingNode() {
        Node updateNode = newBuilder().setNodeId("fakeNode").setHost("newHost").build();
        UpdateServer updateServer = new UpdateServer(updateNode);
        Node initialNode = newBuilder().setNodeId("myNode").setHost("localhost").build();
        Node otherNode = newBuilder().setNodeId("otherNode").build();
        List<Node> nodes = asList(initialNode, otherNode);
        updateServer.apply(nodes);
    }
}