package io.axoniq.axonserver.cluster.grpc;

import io.axoniq.axonserver.cluster.RaftNode;

/**
 * Author: marc
 */
public interface RaftGroupManager {
    RaftNode raftNode(String groupId, String nodeId);
}
