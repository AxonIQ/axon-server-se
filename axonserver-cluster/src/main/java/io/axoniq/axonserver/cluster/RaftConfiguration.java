package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.grpc.cluster.Config;
import io.axoniq.axonserver.grpc.cluster.Node;

import java.util.List;

public interface RaftConfiguration {

    String groupId();

    List<Node> groupMembers();

    void update(List<Node> newConf);

    default Config config() {
        return Config.newBuilder()
                     .addAllNodes(groupMembers())
                     .build();
    }

    default int minElectionTimeout() {
        return 150;
    }

    default int maxElectionTimeout() {
        return 300;
    }

    default int maxReplicationRound() {
        return 10;
    }

    default int heartbeatTimeout() {
        return 50;
    }

    default void clear() {}
}
