package io.axoniq.axonserver.cluster.configuration.operation;

import io.axoniq.axonserver.grpc.cluster.Node;
import org.junit.*;

import java.util.List;

import static io.axoniq.axonserver.grpc.cluster.Node.newBuilder;
import static java.util.Arrays.asList;
import static org.junit.Assert.*;

/**
 * @author Sara Pellegrini
 */
public class RemoveServerTest {

    @Test
    public void testFunction() {
        String nodeId = "myNode";
        RemoveServer removeServer = new RemoveServer(nodeId);
        Node myNode = newBuilder().setNodeId("myNode").build();
        Node otherNode = newBuilder().setNodeId("otherNode").build();
        List<Node> nodes = asList(myNode, otherNode);
        List<Node> result = removeServer.apply(nodes);
        assertEquals(1, result.size());
        assertEquals(otherNode, result.get(0));
    }

    @Test(expected = IllegalArgumentException.class)
    public void removeMissingNode() {
        String nodeId = "fakeNode";
        RemoveServer removeServer = new RemoveServer(nodeId);
        Node myNode = newBuilder().setNodeId("myNode").build();
        Node otherNode = newBuilder().setNodeId("otherNode").build();
        List<Node> nodes = asList(myNode, otherNode);
        List<Node> result = removeServer.apply(nodes);
    }
}