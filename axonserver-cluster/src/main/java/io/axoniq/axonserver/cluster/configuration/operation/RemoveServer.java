package io.axoniq.axonserver.cluster.configuration.operation;

import io.axoniq.axonserver.grpc.cluster.Node;

import java.util.List;
import java.util.function.UnaryOperator;

import static java.util.stream.Collectors.toList;

/**
 * @author Sara Pellegrini
 * @since 4.0
 */
public class RemoveServer implements UnaryOperator<List<Node>> {

    private final String nodeId;

    public RemoveServer(String nodeId) {
        this.nodeId = nodeId;
    }

    @Override
    public List<Node> apply(List<Node> nodes) {
        if (nodes.stream().map(Node::getNodeId).noneMatch(s -> s.equals(nodeId))) {
            throw new IllegalArgumentException(String.format("Node %s is not present", nodeId));
        }
        return nodes.stream().filter(node -> !node.getNodeId().equals(nodeId)).collect(toList());
    }
}
