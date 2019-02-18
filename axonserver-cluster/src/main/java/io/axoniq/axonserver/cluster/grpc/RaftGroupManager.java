package io.axoniq.axonserver.cluster.grpc;

import io.axoniq.axonserver.cluster.RaftNode;

/**
 * @author Marc Gathier
 */
public interface RaftGroupManager {

    /**
     * Finds the raft node for group with id groupId
     * @param groupId the group id
     * @return the raftnode for the group or null when not found
     */
    default RaftNode raftNode(String groupId) {
        return getOrCreateRaftNode(groupId, null);
    }

    /**
     * Finds the raft node for group with id groupId. If it does not exist yet at this node, it will create it
     * and use the specified nodeId for the current node.
     * @param groupId the groupId of the raft group
     * @param nodeId the id to use to register the current node if the group does not exist yet
     * @return the raftnode
     */
    RaftNode getOrCreateRaftNode(String groupId, String nodeId);
}
