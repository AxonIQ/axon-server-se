package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.grpc.cluster.Node;

import java.util.List;

public interface RaftConfiguration {

    List<Node> groupMembers();

    default int minElectionTimeout(){
        return 150;
    }

    default int maxElectionTimeout(){
        return 300;
    }

    String groupId();

    default int heartbeatTimeout() {
        return 50;
    }

    void update(List<Node> nodes);
}
