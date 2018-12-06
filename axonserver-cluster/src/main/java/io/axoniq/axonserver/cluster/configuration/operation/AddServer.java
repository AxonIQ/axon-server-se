package io.axoniq.axonserver.cluster.configuration.operation;

import io.axoniq.axonserver.grpc.cluster.Node;

import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static java.util.stream.Collectors.toList;

/**
 * @author Sara Pellegrini
 * @since 4.0
 */
public class AddServer implements UnaryOperator<List<Node>> {

    private final Node node;

    public AddServer(Node node) {
        this.node = node;
    }


    @Override
    public List<Node> apply(List<Node> nodes) {
        String nodeId = node.getNodeId();
        if (nodes.stream().map(Node::getNodeId).anyMatch(s -> s.equals(nodeId))) {
            throw new IllegalArgumentException(String.format("Node %s already present", nodeId));
        }
        List<Node> result = new LinkedList<>(nodes);
        result.add(node);
        return result;
    }
}
