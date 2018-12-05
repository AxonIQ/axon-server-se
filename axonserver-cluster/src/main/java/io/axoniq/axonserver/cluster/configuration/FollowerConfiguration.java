package io.axoniq.axonserver.cluster.configuration;

import io.axoniq.axonserver.grpc.cluster.ConfigChangeResult;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.cluster.NotLeader;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * @author Sara Pellegrini
 * @since 4.0
 */
public class FollowerConfiguration implements ClusterConfiguration {

    private final Supplier<String> leaderId;

    public FollowerConfiguration(Supplier<String> leaderId) {
        this.leaderId = leaderId;
    }

    @Override
    public CompletableFuture<ConfigChangeResult> addServer(Node node) {
        return leaderId();
    }

    @Override
    public CompletableFuture<ConfigChangeResult> updateServer(Node node) {
        return leaderId();
    }

    @Override
    public CompletableFuture<ConfigChangeResult> removeServer(String nodeId) {
        return leaderId();
    }

    private CompletableFuture<ConfigChangeResult> leaderId() {
        ConfigChangeResult result = ConfigChangeResult.newBuilder()
                                                      .setNotLeader(NotLeader.newBuilder().setLeaderId(leaderId.get()))
                                                      .build();
        return CompletableFuture.completedFuture(result);
    }
}
