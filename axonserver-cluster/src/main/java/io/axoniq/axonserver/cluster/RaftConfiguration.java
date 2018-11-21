package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.grpc.cluster.Node;

import java.util.List;

public interface RaftConfiguration {

    List<Node> groupMembers();

    default long minElectionTimeout(){
        return 150;
    }

    default long maxElectionTimeout(){
        return 300;
    }

    String groupId();

    default long heartbeatTimeout() {
        return 50;
    }

}
