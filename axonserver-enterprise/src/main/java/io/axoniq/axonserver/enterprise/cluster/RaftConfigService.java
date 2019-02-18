package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.grpc.internal.LoadBalanceStrategy;
import io.axoniq.axonserver.grpc.internal.Application;
import io.axoniq.axonserver.grpc.internal.NodeInfo;
import io.axoniq.axonserver.grpc.internal.ProcessorLBStrategy;
import io.axoniq.axonserver.grpc.internal.User;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Author: marc
 */
public interface RaftConfigService {

    void addNodeToContext(String name, String node);

    void deleteContext(String name);

    void deleteNodeFromContext(String name, String node);

    void addContext(String context, List<String> nodes);

    void join(NodeInfo nodeInfo);

    void init(List<String> contexts);

    CompletableFuture<Application> updateApplication(Application application);

    CompletableFuture<Application> refreshToken(Application application);

    CompletableFuture<Void> updateUser(User request);

    CompletableFuture<Void> updateLoadBalancingStrategy(LoadBalanceStrategy loadBalancingStrategy);

    CompletableFuture<Void> deleteLoadBalancingStrategy(LoadBalanceStrategy build);

    CompletableFuture<Void> updateProcessorLoadBalancing(ProcessorLBStrategy processorLBStrategy);

    CompletableFuture<Void> deleteUser(User request);

    CompletableFuture<Void> deleteApplication(Application request);

    void deleteNode(String name);
}
