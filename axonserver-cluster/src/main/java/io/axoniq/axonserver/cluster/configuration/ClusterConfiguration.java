package io.axoniq.axonserver.cluster.configuration;

import io.axoniq.axonserver.grpc.cluster.ConfigChangeResult;
import io.axoniq.axonserver.grpc.cluster.Node;

import java.util.concurrent.CompletableFuture;

/**
 * @author Sara Pellegrini
 * @since 4.0
 */
public interface ClusterConfiguration {

    default CompletableFuture<ConfigChangeResult> addServer(Node node) {
        CompletableFuture result = new CompletableFuture();
        result.completeExceptionally(new UnsupportedOperationException());
        throw new UnsupportedOperationException();
    }

    default CompletableFuture<ConfigChangeResult> updateServer(Node node){
        CompletableFuture result = new CompletableFuture();
        result.completeExceptionally(new UnsupportedOperationException());
        throw new UnsupportedOperationException();
    }

    default CompletableFuture<ConfigChangeResult> removeServer(String nodeId){
        CompletableFuture result = new CompletableFuture();
        result.completeExceptionally(new UnsupportedOperationException());
        throw new UnsupportedOperationException();
    }


}
