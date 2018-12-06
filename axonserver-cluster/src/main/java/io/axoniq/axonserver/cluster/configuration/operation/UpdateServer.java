package io.axoniq.axonserver.cluster.configuration.operation;

import io.axoniq.axonserver.grpc.cluster.Node;

import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * @author Sara Pellegrini
 * @since 4.0
 */
public class UpdateServer implements UnaryOperator<List<Node>> {

    private final Node node;

    public UpdateServer(Node node) {
        this.node = node;
    }

    @Override
    public List<Node> apply(List<Node> nodes) {
        String nodeId = node.getNodeId();
        if (nodes.stream().map(Node::getNodeId).noneMatch(s -> s.equals(nodeId))) {
            throw new IllegalArgumentException(String.format("Node %s not present", nodeId));
        }
        List<Node> newConfiguration = new LinkedList<>();
        newConfiguration.add(node);
        for (Node n : nodes) {
            if (!n.getNodeId().equals(node.getNodeId())) {
                newConfiguration.add(n);
            }
        }
        return newConfiguration;
    }

}
