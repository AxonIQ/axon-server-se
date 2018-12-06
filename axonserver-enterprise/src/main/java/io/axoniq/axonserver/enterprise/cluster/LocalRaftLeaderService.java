package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.cluster.RaftNode;
import io.axoniq.axonserver.grpc.cluster.Node;

import java.util.concurrent.CompletableFuture;

/**
 * Author: marc
 */
public class LocalRaftLeaderService implements RaftLeaderService {

    private final String context;
    private final RaftNode raftNode;

    public LocalRaftLeaderService(String context, RaftNode raftNode) {

        this.context = context;
        this.raftNode = raftNode;
    }

    @Override
    public CompletableFuture<Void> addNodeToContext(Node me) {
        return raftNode.addNode(me);
    }
}
