package io.axoniq.axonserver.enterprise.cluster;

import com.google.protobuf.ByteString;
import io.axoniq.axonserver.grpc.Confirmation;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.internal.Context;
import io.axoniq.axonserver.grpc.internal.ContextApplication;
import io.axoniq.axonserver.grpc.internal.ContextEntry;
import io.axoniq.axonserver.grpc.internal.ContextLoadBalanceStrategy;
import io.axoniq.axonserver.grpc.internal.ContextMember;
import io.axoniq.axonserver.grpc.internal.ContextName;
import io.axoniq.axonserver.grpc.internal.ContextProcessorLBStrategy;
import io.axoniq.axonserver.grpc.internal.LoadBalanceStrategy;
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
 * @author Marc Gathier
 */
public class RemoteRaftGroupService implements RaftGroupService {
    private static final Function<Confirmation, Void> TO_VOID = x -> null;
    private static final Logger logger = LoggerFactory.getLogger(RemoteRaftGroupService.class);

    private final RaftGroupServiceGrpc.RaftGroupServiceStub stub;

    public RemoteRaftGroupService( RaftGroupServiceGrpc.RaftGroupServiceStub stub) {
        this.stub = stub;
    }

    @Override
    public CompletableFuture<Void> addNodeToContext(String context, Node node) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        ContextMember contextMember = asContextMember(node);
        stub.addServer(Context.newBuilder().setName(context).addMembers(contextMember).build(),
                       new CompletableStreamObserver<>(result, logger, TO_VOID));
        return result;
    }

    @Override
    public CompletableFuture<Void> deleteNode(String context, String node) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        stub.removeServer(Context.newBuilder().setName(context)
                                          .addMembers(ContextMember.newBuilder().setNodeId(node).build()).build(),
                          new CompletableStreamObserver<>(result, logger,  TO_VOID));
        return result;
    }

    @Override
    public CompletableFuture<Void> updateApplication(ContextApplication application) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        stub.mergeAppAuthorization(application,
                                   new CompletableStreamObserver<>(result, logger));
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
                         new CompletableStreamObserver<>(result, logger));
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
                logger.debug("Failed to retrieve status");
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
                                 .addAllMembers(raftNodes.stream().map(this::asContextMember).collect(Collectors.toList()))
                                 .build();
        stub.initContext(
                request, new CompletableStreamObserver<>(result, logger, TO_VOID));

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
                                      new CompletableStreamObserver<>(result, logger, TO_VOID));
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
                                      new CompletableStreamObserver<>(result, logger, TO_VOID));
        return result;
    }

    @Override
    public CompletableFuture<Void> deleteLoadBalancingStrategy(String context,
                                                               LoadBalanceStrategy loadBalancingStrategy) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        stub.deleteLoadBalanceStrategy(ContextLoadBalanceStrategy.newBuilder()
                                                                 .setLoadBalanceStrategy(loadBalancingStrategy)
                                                                 .setContext(context)
                                                                 .build(), new CompletableStreamObserver<>(result, logger ,TO_VOID));
        return result;
    }

    @Override
    public CompletableFuture<Void> deleteContext(String context) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        stub.deleteContext(ContextName.newBuilder().setContext(context).build(), new CompletableStreamObserver(result, logger,TO_VOID));
        return result;
    }
}
