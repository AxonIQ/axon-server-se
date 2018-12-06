package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.grpc.cluster.Node;

import java.util.concurrent.CompletableFuture;

/**
 * Author: marc
 */
public interface RaftLeaderService {

    CompletableFuture<Void> addNodeToContext(Node me);
}
