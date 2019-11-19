package io.axoniq.axonserver.enterprise.cluster;

import com.google.protobuf.ByteString;
import io.axoniq.axonserver.grpc.ContextMemberConverter;
import io.axoniq.axonserver.grpc.GrpcExceptionBuilder;
import io.axoniq.axonserver.grpc.InstructionAck;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.internal.Context;
import io.axoniq.axonserver.grpc.internal.ContextApplication;
import io.axoniq.axonserver.grpc.internal.ContextConfiguration;
import io.axoniq.axonserver.grpc.internal.ContextEntry;
import io.axoniq.axonserver.grpc.internal.ContextLoadBalanceStrategy;
import io.axoniq.axonserver.grpc.internal.ContextMember;
import io.axoniq.axonserver.grpc.internal.ContextName;
import io.axoniq.axonserver.grpc.internal.ContextProcessorLBStrategy;
import io.axoniq.axonserver.grpc.internal.ContextUpdateConfirmation;
import io.axoniq.axonserver.grpc.internal.ContextUser;
import io.axoniq.axonserver.grpc.internal.DeleteContextRequest;
import io.axoniq.axonserver.grpc.internal.LoadBalanceStrategy;
import io.axoniq.axonserver.grpc.internal.NodeContext;
import io.axoniq.axonserver.grpc.internal.ProcessorLBStrategy;
import io.axoniq.axonserver.grpc.internal.RaftGroupServiceGrpc;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * RaftGroupService to use when the leader of the raft group is on a remote node. Sends all requests through GRPC calls.
 * @author Marc Gathier
 */
public class RemoteRaftGroupService implements RaftGroupService {

    private static final Function<InstructionAck, Void> TO_VOID = x -> null;
    private static final Logger logger = LoggerFactory.getLogger(RemoteRaftGroupService.class);

    private final RaftGroupServiceGrpc.RaftGroupServiceStub stub;

    public RemoteRaftGroupService( RaftGroupServiceGrpc.RaftGroupServiceStub stub) {
        this.stub = stub;
    }

    @Override
    public CompletableFuture<ContextUpdateConfirmation> addNodeToContext(String context, Node node) {
        CompletableFuture<ContextUpdateConfirmation> result = new CompletableFuture<>();
        ContextMember contextMember = ContextMemberConverter.asContextMember(node);
        stub.addServer(Context.newBuilder().setName(context).addMembers(contextMember).build(),
                       new CompletableStreamObserver<>(result, "addNodeToContext", logger));
        return result;
    }

    @Override
    public CompletableFuture<ContextUpdateConfirmation> deleteNode(String context, String node) {
        CompletableFuture<ContextUpdateConfirmation> result = new CompletableFuture<>();
        stub.removeServer(Context.newBuilder().setName(context)
                                          .addMembers(ContextMember.newBuilder().setNodeId(node).build()).build(),
                          new CompletableStreamObserver<>(result, "deleteNode", logger));
        return result;
    }

    @Override
    public CompletableFuture<Void> updateApplication(ContextApplication application) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        stub.mergeAppAuthorization(application,
                                   new CompletableStreamObserver<>(result, "updateApplication", logger));
        return result;
    }


    @Override
    public CompletableFuture<Void> appendEntry(String context, String name, byte[] bytes) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        ContextEntry request = ContextEntry.newBuilder()
                                           .setContext(context)
                                           .setEntryName(name)
                                           .setEntry(ByteString.copyFrom(bytes))
                                           .build();
        stub.appendEntry(request,
                         new CompletableStreamObserver<>(result, "appendEntry", logger));
        return result;
    }

    @Override
    public CompletableFuture<Void> getStatus(Consumer<Context> contextConsumer) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        stub.getStatus(Context.getDefaultInstance(), new StreamObserver<Context>() {
            @Override
            public void onNext(Context context) {
                contextConsumer.accept(context);
            }

            @Override
            public void onError(Throwable throwable) {
                // log only
                logger.debug("Failed to retrieve status");
                result.complete(null);
            }

            @Override
            public void onCompleted() {
                // no further action needed
                result.complete(null);
            }
        });

        return result;
    }

    @Override
    public CompletableFuture<ContextConfiguration> initContext(String context, List<Node> raftNodes) {
        CompletableFuture<ContextConfiguration> result = new CompletableFuture<>();
        Context request = Context.newBuilder()
                                 .setName(context)
                                 .addAllMembers(raftNodes.stream().map(this::asContextMember).collect(Collectors.toList()))
                                 .build();
        stub.initContext(
                request, new CompletableStreamObserver<>(result, "initContext", logger));

        return result;
    }

    private ContextMember asContextMember(Node r) {
        return ContextMember.newBuilder()
                            .setHost(r.getHost())
                            .setPort(r.getPort())
                            .setNodeId(r.getNodeId())
                            .setNodeName(r.getNodeName())
                            .build();
    }

    @Override
    public CompletableFuture<Void> updateLoadBalancingStrategy(String name, LoadBalanceStrategy loadBalancingStrategy) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        stub.mergeLoadBalanceStrategy(ContextLoadBalanceStrategy.newBuilder()
                                                                .setContext(name)
                                                                .setLoadBalanceStrategy(loadBalancingStrategy)
                                                                .build(),
                                      new CompletableStreamObserver<>(result,
                                                                      "updateLoadBalancingStrategy",
                                                                      logger,
                                                                      TO_VOID));
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
                                      new CompletableStreamObserver<>(result,
                                                                      "updateProcessorLoadBalancing",
                                                                      logger,
                                                                      TO_VOID));
        return result;
    }

    @Override
    public CompletableFuture<Void> deleteLoadBalancingStrategy(String context,
                                                               LoadBalanceStrategy loadBalancingStrategy) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        stub.deleteLoadBalanceStrategy(ContextLoadBalanceStrategy.newBuilder()
                                                                 .setLoadBalanceStrategy(loadBalancingStrategy)
                                                                 .setContext(context)
                                                                 .build(),
                                       new CompletableStreamObserver<>(result,
                                                                       "deleteLoadBalancingStrategy",
                                                                       logger,
                                                                       TO_VOID));
        return result;
    }

    @Override
    public CompletableFuture<Void> deleteContext(String context, boolean preserveEventStore) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        stub.deleteContext(DeleteContextRequest.newBuilder()
                                               .setContext(context)
                                               .setPreserveEventstore(preserveEventStore)
                                               .build(), new StreamObserver<InstructionAck>() {
            @Override
            public void onNext(InstructionAck value) {
                result.complete(null);
            }

            @Override
            public void onError(Throwable throwable) {
                logger.warn("Remote action failed", throwable);
                result.completeExceptionally(GrpcExceptionBuilder.parse(throwable));
            }

            @Override
            public void onCompleted() {

            }
        });
        return result;
    }

    @Override
    public CompletableFuture<ContextConfiguration> configuration(String context) {
        CompletableFuture<ContextConfiguration> result = new CompletableFuture<>();
        stub.configuration(ContextName.newBuilder().setContext(context).build(),
                           new CompletableStreamObserver<>(result, "configuration", logger));
        return result;
    }

    @Override
    public CompletableFuture<Void> transferLeadership(String context) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        stub.transferLeadership(ContextName.newBuilder().setContext(context).build(),
                                new CompletableStreamObserver<>(result, "transferLeadership", logger, TO_VOID));
        return result;
    }

    @Override
    public CompletableFuture<Void> deleteApplication(ContextApplication application) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        stub.deleteAppAuthorization(application,
                                    new CompletableStreamObserver<>(result, "deleteAppAuthorization", logger, TO_VOID));
        return result;
    }

    @Override
    public CompletableFuture<Void> updateUser(ContextUser user) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        stub.mergeUserAuthorization(user,
                                    new CompletableStreamObserver<>(result, "updateUser", logger, TO_VOID));
        return result;
    }

    @Override
    public CompletableFuture<Void> deleteUser(ContextUser user) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        stub.deleteUserAuthorization(user,
                                     new CompletableStreamObserver<>(result, "deleteUser", logger, TO_VOID));
        return result;
    }

    @Override
    public CompletableFuture<Void> prepareDeleteNodeFromContext(String context, String node) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        stub.preDeleteNodeFromContext(NodeContext.newBuilder()
                                                 .setNodeName(node)
                                                 .setContext(context)
                                                 .build(),
                                      new CompletableStreamObserver<>(result,
                                                                      "prepareDeleteNodeFromContext",
                                                                      logger,
                                                                      TO_VOID));
        return result;
    }
}
