package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.grpc.cluster.Node;

import java.util.List;

public interface RaftConfiguration {

    List<Node> groupMembers();

    // TODO: Methods for membership changes
}
