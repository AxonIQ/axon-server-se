package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.grpc.cluster.Config;
import io.axoniq.axonserver.grpc.cluster.Node;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author Sara Pellegrini
 * @since
 */
public class FakeRaftConfiguration implements RaftConfiguration {

    private final String groupId;
    private final Map<String,Node> members = new ConcurrentHashMap<>();

    public FakeRaftConfiguration(String groupId, String localNodeId) {
        this.groupId = groupId;
        addNode(Node.newBuilder().setNodeId(localNodeId).build());
    }

    public void addNode(Node node){
        this.members.put(node.getNodeId(), node);
    }

    @Override
    public List<Node> groupMembers() {
        return new ArrayList<>(members.values());
    }

    @Override
    public String groupId() {
        return groupId;
    }

    @Override
    public void update(List<Node> nodes) {
        members.clear();
        nodes.forEach(this::addNode);
    }
}
