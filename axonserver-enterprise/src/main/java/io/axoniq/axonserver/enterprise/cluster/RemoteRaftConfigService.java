package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.grpc.Confirmation;
import io.axoniq.axonserver.grpc.GrpcExceptionBuilder;
import io.axoniq.axonserver.grpc.internal.Application;
import io.axoniq.axonserver.grpc.internal.Context;
import io.axoniq.axonserver.grpc.internal.ContextMember;
import io.axoniq.axonserver.grpc.internal.ContextName;
import io.axoniq.axonserver.grpc.internal.ContextNames;
import io.axoniq.axonserver.grpc.internal.LoadBalanceStrategy;
import io.axoniq.axonserver.grpc.internal.NodeContext;
import io.axoniq.axonserver.grpc.internal.NodeInfo;
import io.axoniq.axonserver.grpc.internal.NodeName;
import io.axoniq.axonserver.grpc.internal.ProcessorLBStrategy;
import io.axoniq.axonserver.grpc.internal.RaftConfigServiceGrpc;
import io.axoniq.axonserver.grpc.internal.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Author: marc
 */
public class RemoteRaftConfigService implements RaftConfigService {
    private static final Logger logger = LoggerFactory.getLogger(RemoteRaftConfigService.class);
    private static final Function<Confirmation, Void> TO_VOID = x -> null;

    private final RaftConfigServiceGrpc.RaftConfigServiceStub raftConfigServiceStub;

    public RemoteRaftConfigService(RaftConfigServiceGrpc.RaftConfigServiceStub raftConfigServiceStub) {
        this.raftConfigServiceStub = raftConfigServiceStub;
    }

    @Override
    public void addNodeToContext(String context, String node) {
        CompletableFuture<Confirmation> completableFuture = new CompletableFuture<>();
        raftConfigServiceStub.addNodeToContext(NodeContext.newBuilder().setNodeName(node).setContext(context).build(),
                                               new CompletableStreamObserver<>(completableFuture, logger));
        wait(completableFuture);
    }

    @Override
    public void deleteNode(String name) {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        raftConfigServiceStub.deleteNode(NodeName.newBuilder().setNode(name).build(), new CompletableStreamObserver(completableFuture, logger));
        wait(completableFuture);
    }

    @Override
    public void deleteContext(String context) {
        CompletableFuture<Confirmation> completableFuture = new CompletableFuture<>();
        raftConfigServiceStub.deleteContext(ContextName.newBuilder().setContext(context).build(), new CompletableStreamObserver<>(completableFuture, logger));
        wait(completableFuture);
    }

    private void wait(CompletableFuture<?> completableFuture) {
        try {
            completableFuture.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw GrpcExceptionBuilder.build(e);
        } catch (ExecutionException e) {
            throw GrpcExceptionBuilder.build(e.getCause());
        }
    }

    @Override
    public void deleteNodeFromContext(String context, String node) {
        CompletableFuture<Confirmation> completableFuture = new CompletableFuture<>();
        raftConfigServiceStub.deleteNodeFromContext(NodeContext.newBuilder().setNodeName(node).setContext(context).build(),
                                               new CompletableStreamObserver<>(completableFuture, logger));
        wait(completableFuture);
    }

    @Override
    public void addContext(String context, List<String> nodes) {
        CompletableFuture<Confirmation> completableFuture = new CompletableFuture<>();
        raftConfigServiceStub.createContext(Context
                                                    .newBuilder()
                                                    .setName(context)
                                                    .addAllMembers(nodes.stream().map(n -> ContextMember.newBuilder().setNodeId(n).build()).collect(
                                                            Collectors.toList()))
                                                    .build(), new CompletableStreamObserver<>(completableFuture, logger));
        wait(completableFuture);
    }

    @Override
    public void join(NodeInfo nodeInfo) {
        CompletableFuture<Confirmation> completableFuture = new CompletableFuture<>();
        raftConfigServiceStub.joinCluster(nodeInfo, new CompletableStreamObserver<>(completableFuture, logger));
        wait(completableFuture);
    }

    @Override
    public void init(List<String> contexts) {
        CompletableFuture<Confirmation> completableFuture = new CompletableFuture<>();
        raftConfigServiceStub.initCluster(ContextNames.newBuilder().addAllContexts(contexts).build(), new CompletableStreamObserver<>(completableFuture, logger));
        wait(completableFuture);

    }

    @Override
    public CompletableFuture<Application> updateApplication(Application application) {
        CompletableFuture<Application> returnedApplication = new CompletableFuture<>();
        raftConfigServiceStub.updateApplication(application, new CompletableStreamObserver<>(returnedApplication, logger));
        return returnedApplication;
    }

    @Override
    public CompletableFuture<Application> refreshToken(Application application) {
        CompletableFuture<Application> returnedApplication = new CompletableFuture<>();
        raftConfigServiceStub.refreshToken(application, new CompletableStreamObserver<>(returnedApplication, logger));
        return returnedApplication;
    }


    @Override
    public CompletableFuture<Void> updateUser(User request) {
        CompletableFuture<Void> confirmation = new CompletableFuture<>();
        raftConfigServiceStub.updateUser(request, new CompletableStreamObserver<>(confirmation, logger, TO_VOID));
        return confirmation;
    }

    @Override
    public CompletableFuture<Void> updateLoadBalancingStrategy(LoadBalanceStrategy loadBalancingStrategy) {
        CompletableFuture<Void> confirmation = new CompletableFuture<>();
        raftConfigServiceStub.updateLoadBalanceStrategy(loadBalancingStrategy, new CompletableStreamObserver<>(confirmation, logger, TO_VOID));
        return confirmation;
    }

    @Override
    public CompletableFuture<Void> deleteLoadBalancingStrategy(LoadBalanceStrategy loadBalancingStrategy) {
        CompletableFuture<Void> confirmation = new CompletableFuture<>();
        raftConfigServiceStub.deleteLoadBalanceStrategy(loadBalancingStrategy, new CompletableStreamObserver(confirmation, logger, TO_VOID));
        return confirmation;
    }

    @Override
    public CompletableFuture<Void> updateProcessorLoadBalancing(ProcessorLBStrategy processorLBStrategy) {
        CompletableFuture<Void> confirmation = new CompletableFuture<>();
        raftConfigServiceStub.updateProcessorLBStrategy(processorLBStrategy, new CompletableStreamObserver<>(confirmation, logger, TO_VOID));
        return confirmation;
    }

    @Override
    public CompletableFuture<Void> deleteUser(User user) {
        CompletableFuture<Void> confirmation = new CompletableFuture<>();
        raftConfigServiceStub.deleteUser(user, new CompletableStreamObserver<>(confirmation, logger, TO_VOID));
        return confirmation;
    }

    @Override
    public CompletableFuture<Void> deleteApplication(Application application) {
        CompletableFuture<Void> confirmation = new CompletableFuture<>();
        raftConfigServiceStub.deleteApplication(application, new CompletableStreamObserver<>(confirmation, logger, TO_VOID));
        return confirmation;
    }
}
