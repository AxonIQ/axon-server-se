package io.axoniq.axonserver.cluster.configuration;

import io.axoniq.axonserver.grpc.cluster.ConfigChangeResult;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.cluster.NotLeader;

import java.util.concurrent.CompletableFuture;

/**
 * @author Sara Pellegrini
 * @since 4.0
 */
public class CandidateConfiguration implements ClusterConfiguration {

    private final ConfigChangeResult result = ConfigChangeResult.newBuilder()
                                                                .setNotLeader(NotLeader.newBuilder())
                                                                .build();

    @Override
    public CompletableFuture<ConfigChangeResult> addServer(Node node) {
        return notLeader();
    }

    @Override
    public CompletableFuture<ConfigChangeResult> removeServer(String nodeId) {
        return notLeader();
    }

    private CompletableFuture<ConfigChangeResult> notLeader() {
        return CompletableFuture.completedFuture(result);
    }
}
