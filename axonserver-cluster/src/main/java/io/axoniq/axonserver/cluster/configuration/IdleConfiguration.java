package io.axoniq.axonserver.cluster.configuration;

import io.axoniq.axonserver.cluster.exception.ServerNotReadyException;
import io.axoniq.axonserver.grpc.cluster.ConfigChangeResult;
import io.axoniq.axonserver.grpc.cluster.Node;

import java.util.concurrent.CompletableFuture;

/**
 * @author Sara Pellegrini
 * @since 4.0
 */
public class IdleConfiguration implements ClusterConfiguration {

    @Override
    public CompletableFuture<ConfigChangeResult> addServer(Node node) {
        return serverNotReady();
    }

    @Override
    public CompletableFuture<ConfigChangeResult> updateServer(Node node) {
        return serverNotReady();
    }

    @Override
    public CompletableFuture<ConfigChangeResult> removeServer(String nodeId) {
        return serverNotReady();
    }

    private CompletableFuture<ConfigChangeResult> serverNotReady(){
        CompletableFuture<ConfigChangeResult> result = new CompletableFuture<>();
        String message = "The server is in an idle state and can not handle configuration change requests.";
        result.completeExceptionally(new ServerNotReadyException(message));
        return result;
    }
}
