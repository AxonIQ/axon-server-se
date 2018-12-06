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
public class AddServerTest {

    @Test
    public void addNodeToEmptyList() {
        Node node = Node.newBuilder().build();
        AddServer addServer = new AddServer(node);
        List<Node> result = addServer.apply(Collections.emptyList());
        assertEquals(1, result.size());
        assertEquals(node, result.get(0));
    }

    @Test
    public void addNode() {
        Node myNode = newBuilder().setNodeId("myNode").build();
        AddServer addServer = new AddServer(myNode);
        Node otherNode = newBuilder().setNodeId("otherNode").build();
        List<Node> nodes = asList(otherNode);
        List<Node> result = addServer.apply(nodes);
        assertEquals(2, result.size());
        assertTrue(result.contains(myNode));
    }

    @Test(expected = IllegalArgumentException.class)
    public void addNodeAlreadyExisting() {
        Node myNode = newBuilder().setNodeId("myNode").build();
        AddServer addServer = new AddServer(myNode);
        Node otherNode = newBuilder().setNodeId("otherNode").build();
        List<Node> nodes = asList(myNode, otherNode);
        addServer.apply(nodes);
    }
}