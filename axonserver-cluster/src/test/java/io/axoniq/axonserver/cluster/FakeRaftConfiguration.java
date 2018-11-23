package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.grpc.cluster.Config;
import io.axoniq.axonserver.grpc.cluster.Node;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author Sara Pellegrini
 * @since
 */
public class FakeRaftConfiguration implements RaftConfiguration {

    private final String groupId;
    private final List<Node> members = new CopyOnWriteArrayList<>();

    public FakeRaftConfiguration(String groupId) {
        this.groupId = groupId;
    }

    public void addNode(Node node){
        this.members.add(node);
    }

    @Override
    public List<Node> groupMembers() {
        return members;
    }

    @Override
    public String groupId() {
        return groupId;
    }

    @Override
    public void update(List<Node> nodes) {
        members.clear();
        members.addAll(nodes);
    }
}
