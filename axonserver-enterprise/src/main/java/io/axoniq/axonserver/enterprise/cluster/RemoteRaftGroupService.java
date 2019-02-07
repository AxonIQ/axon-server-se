package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.grpc.Confirmation;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.internal.Context;
import io.axoniq.axonserver.grpc.internal.ContextApplication;
import io.axoniq.axonserver.grpc.internal.ContextLoadBalanceStrategy;
import io.axoniq.axonserver.grpc.internal.ContextMember;
import io.axoniq.axonserver.grpc.internal.ContextProcessorLBStrategy;
import io.axoniq.axonserver.grpc.internal.ContextUser;
import io.axoniq.axonserver.grpc.internal.LoadBalanceStrategy;
import io.axoniq.axonserver.grpc.internal.ProcessorLBStrategy;
import io.axoniq.axonserver.grpc.internal.RaftGroupServiceGrpc;
import io.axoniq.axonserver.grpc.internal.User;
import io.grpc.stub.StreamObserver;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Author: marc
 */
public class RemoteRaftGroupService implements RaftGroupService {
    private static final Function<Confirmation, Void> TO_VOID = x -> null;

    private final RaftGroupServiceGrpc.RaftGroupServiceStub stub;

    public RemoteRaftGroupService( RaftGroupServiceGrpc.RaftGroupServiceStub stub) {
        this.stub = stub;
    }

    @Override
    public CompletableFuture<Void> addNodeToContext(String context, Node node) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        ContextMember contextMember = ContextMember.newBuilder()
                                                   .setHost(node.getHost())
                                                   .setNodeId(node.getNodeId())
                                                   .setPort(node.getPort())
                                                   .build();
        stub.addServer(Context.newBuilder().setName(context).addMembers(contextMember).build(),
                       new CompletableStreamObserver<>(result, TO_VOID));
        return result;
    }

    @Override
    public CompletableFuture<Void> deleteNode(String context, String node) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        stub.removeServer(Context.newBuilder().setName(context)
                                          .addMembers(ContextMember.newBuilder().setNodeId(node).build()).build(),
                          new CompletableStreamObserver<>(result, TO_VOID));
        return result;
    }

    @Override
    public CompletableFuture<Void> updateApplication(ContextApplication application) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        stub.mergeAppAuthorization(application,
                                   new CompletableStreamObserver<>(result));
        return result;
    }

    @Override
    public CompletableFuture<Void> updateUser(String context, User request) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        stub.mergeUserAuthorization(ContextUser.newBuilder().setContext(context).setUser(request).build(), new CompletableStreamObserver<>(result, TO_VOID));
        return result;
    }

    @Override
    public void getStatus(Consumer<Context> contextConsumer) {
        stub.getStatus(Context.getDefaultInstance(), new StreamObserver<Context>() {
            @Override
            public void onNext(Context context) {
                contextConsumer.accept(context);
            }

            @Override
            public void onError(Throwable throwable) {
                // log only
            }

            @Override
            public void onCompleted() {
                // no further action needed
            }
        });

    }

    @Override
    public CompletableFuture<Void> initContext(String context, List<Node> raftNodes) {
        CompletableFuture<Void> result = new CompletableFuture<>();
            Context request = Context.newBuilder()
                                     .setName(context)
                                     .addAllMembers(raftNodes.stream().map(r -> ContextMember.newBuilder()
                                                                                   .setHost(r.getHost())
                                                                           .setPort(r.getPort())
                                                                           .setNodeId(r.getNodeId())
                                                                           .build()
                                             ).collect(Collectors.toList()))
                                     .build();
        stub.initContext(
                    request,new CompletableStreamObserver<>(result, TO_VOID));

        return result;
    }

    @Override
    public CompletableFuture<Void> updateLoadBalancingStrategy(String name, LoadBalanceStrategy loadBalancingStrategy) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        stub.mergeLoadBalanceStrategy(ContextLoadBalanceStrategy.newBuilder()
                                                                .setContext(name)
                                                                .setLoadBalanceStrategy(loadBalancingStrategy)
                                                                .build(),
                                      new CompletableStreamObserver<>(result, TO_VOID));
        return result;
    }

    @Override
    public CompletableFuture<Void> updateProcessorLoadBalancing(String context,
                                                                ProcessorLBStrategy processorLBStrategy) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        stub.mergeProcessorLBStrategy(ContextProcessorLBStrategy.newBuilder()
                                                                .setContext(context)
                                                                .setProcessorLBStrategy(processorLBStrategy)
                                                                .build(),
                                      new CompletableStreamObserver<>(result, TO_VOID));
        return result;
    }

    @Override
    public CompletableFuture<Void> deleteApplication(ContextApplication application) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        stub.deleteAppAuthorization(application, new CompletableStreamObserver<>(result, TO_VOID));
        return result;
    }

    @Override
    public CompletableFuture<Void> deleteUser(String context, User request) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        stub.deleteUserAuthorization(ContextUser.newBuilder()
                                                      .setUser(request)
                                                      .setContext(context)
                                                      .build(), new CompletableStreamObserver<>(result, TO_VOID));
        return result;
    }

    @Override
    public CompletableFuture<Void> deleteLoadBalancingStrategy(String context,
                                                               LoadBalanceStrategy loadBalancingStrategy) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        stub.deleteLoadBalanceStrategy(ContextLoadBalanceStrategy.newBuilder()
                                                                 .setLoadBalanceStrategy(loadBalancingStrategy)
                                                                 .setContext(context)
                                                                 .build(), new CompletableStreamObserver<>(result, TO_VOID));
        return result;
    }

    @Override
    public CompletableFuture<Void> deleteContext(String context) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        //TODO
        result.completeExceptionally(new NoSuchMethodException("Not implemented yet"));
        return result;
    }
}
