package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.internal.Context;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Author: marc
 */
public interface RaftGroupService {

    CompletableFuture<Void> addNodeToContext(String context, Node node);

    void getStatus(Consumer<Context> contextConsumer);

    CompletableFuture<Void> initContext(String context, List<Node> nodes);

    CompletableFuture<Void> deleteNode(String context, String node);
}
