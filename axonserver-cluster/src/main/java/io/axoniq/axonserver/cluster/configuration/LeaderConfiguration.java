package io.axoniq.axonserver.cluster.configuration;

import io.axoniq.axonserver.cluster.RaftGroup;
import io.axoniq.axonserver.cluster.configuration.operation.AddServer;
import io.axoniq.axonserver.cluster.configuration.operation.RemoveServer;
import io.axoniq.axonserver.cluster.exception.RaftErrorMapping;
import io.axoniq.axonserver.grpc.cluster.ConfigChangeFailure;
import io.axoniq.axonserver.grpc.cluster.ConfigChangeResult;
import io.axoniq.axonserver.grpc.cluster.ConfigChangeSuccess;
import io.axoniq.axonserver.grpc.cluster.ErrorMessage;
import io.axoniq.axonserver.grpc.cluster.Node;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.concurrent.CompletableFuture.completedFuture;

/**
 * @author Sara Pellegrini
 * @since 4.0
 */
public class LeaderConfiguration implements ClusterConfiguration {

    private final Supplier<String> localNode;

    private final Function<Node, CompletableFuture<Void>> updateNode;

    private final Function<UnaryOperator<List<Node>>, CompletableFuture<Void>> appendConfigurationChange;

    private final Function<Throwable, ErrorMessage> errorMapping;

    public LeaderConfiguration(RaftGroup raftGroup,
                               Supplier<Long> currentTime,
                               Function<Node, NodeReplicator> replicatorFactory,
                               Function<UnaryOperator<List<Node>>, CompletableFuture<Void>> appendConfigurationChange) {
        this(() -> raftGroup.localNode().nodeId(),
             appendConfigurationChange,
             new UpdatePendingMember(raftGroup, currentTime, replicatorFactory));
    }

    public LeaderConfiguration(
            Supplier<String> localNode,
            Function<UnaryOperator<List<Node>>, CompletableFuture<Void>> appendConfigurationChange,
            Function<Node, CompletableFuture<Void>> updateNode) {
        this(localNode, appendConfigurationChange, updateNode, new RaftErrorMapping());
    }

    public LeaderConfiguration(Supplier<String> localNode,
                               Function<UnaryOperator<List<Node>>, CompletableFuture<Void>> appendConfigurationChange,
                               Function<Node, CompletableFuture<Void>> updateNode,
                               Function<Throwable, ErrorMessage> errorMapping) {
        this.localNode = localNode;
        this.appendConfigurationChange = appendConfigurationChange;
        this.updateNode = updateNode;
        this.errorMapping = errorMapping;
    }

    @Override
    public CompletableFuture<ConfigChangeResult> addServer(Node node) {
        checkValidityOf(node);
        return update(node).thenCompose(success -> appendConfigurationChange.apply(new AddServer(node)))
                           .thenApply(success -> success())
                           .exceptionally(this::failure);
    }

    private CompletableFuture<Void> update(Node node) {
        if (node.getNodeId().equals(localNode.get())) {
            return completedFuture(null);
        }
        return updateNode.apply(node);
    }

    private void checkValidityOf(Node node) {
        checkArgument(!node.getNodeId().isEmpty(), "nodeId cannot be empty");
        checkArgument(!node.getHost().isEmpty(), "host cannot be empty");
        checkArgument(node.getPort() != 0, "port cannot be 0");
    }

    @Override
    public CompletableFuture<ConfigChangeResult> removeServer(String nodeId) {
        checkArgument(nodeId != null, "nodeId cannot be null");
        checkArgument(!nodeId.isEmpty(), "nodeId cannot be empty");
        return appendConfigurationChange.apply(new RemoveServer(nodeId))
                                        .thenApply(success -> success())
                                        .exceptionally(this::failure);
    }

    private ConfigChangeResult success() {
        return ConfigChangeResult
                .newBuilder()
                .setSuccess(ConfigChangeSuccess.newBuilder().build())
                .build();
    }

    private ConfigChangeResult failure(Throwable error) {
        return ConfigChangeResult
                .newBuilder()
                .setFailure(ConfigChangeFailure.newBuilder().setError(errorMapping.apply(error.getCause())))
                .build();
    }
}
