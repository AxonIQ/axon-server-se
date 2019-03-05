package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.cluster.RaftGroup;
import io.axoniq.axonserver.cluster.RaftNode;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.internal.Context;
import io.axoniq.axonserver.grpc.internal.ContextApplication;
import io.axoniq.axonserver.grpc.internal.ContextMember;
import io.axoniq.axonserver.grpc.internal.LoadBalanceStrategy;
import io.axoniq.axonserver.grpc.internal.ProcessorLBStrategy;
import io.axoniq.axonserver.grpc.internal.State;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static io.axoniq.axonserver.enterprise.logconsumer.DeleteLoadBalancingStrategyConsumer.DELETE_LOAD_BALANCING_STRATEGY;

/**
 * Author: marc
 */
@Component
public class LocalRaftGroupService implements RaftGroupService {

    private GrpcRaftController grpcRaftController;

    public LocalRaftGroupService(GrpcRaftController grpcRaftController) {
        this.grpcRaftController = grpcRaftController;
    }

    @Override
    public CompletableFuture<Void> addNodeToContext(String context, Node node) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        RaftNode raftNode = grpcRaftController.getRaftNode(context);
        raftNode.addNode(node).whenComplete(((configChangeResult, throwable) -> {
            if(throwable != null) {
                result.completeExceptionally(throwable);
            } else {
                if( configChangeResult.hasFailure()) {
                    result.completeExceptionally(new RuntimeException(configChangeResult.getFailure().toString()));
                } else {
                    result.complete(null);
                }
            }

        }));

        return result;
    }

    @Override
    public void getStatus(Consumer<Context> contextConsumer) {
        grpcRaftController.raftGroups().forEach(name -> contextConsumer.accept(getStatus(name)));
    }

    @Override
    public CompletableFuture<Void> deleteNode(String context, String node) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        RaftNode raftNode = grpcRaftController.getRaftNode(context);
        raftNode.removeNode(node).whenComplete(((configChangeResult, throwable) -> {
            if(throwable != null) {
                result.completeExceptionally(throwable);
            } else {
                if( configChangeResult.hasFailure()) {
                    result.completeExceptionally(new RuntimeException(configChangeResult.getFailure().toString()));
                } else {
                    result.complete(null);
                }
            }

        }));

        return result;
    }

    @Override
    public CompletableFuture<Void> initContext(String context, List<Node> raftNodes) {
        RaftGroup raftGroup = grpcRaftController.initRaftGroup(context, grpcRaftController.getMyLabel(raftNodes),
                                                               grpcRaftController.getMyName());
        RaftNode leader = grpcRaftController.waitForLeader(raftGroup);
        raftNodes.forEach(n -> {
            try {
                leader.addNode(n).get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e.getCause());
            }
        });

        return CompletableFuture.completedFuture(null);
    }


    public Context getStatus(String name) {
        RaftGroup raftGroup = grpcRaftController.getRaftGroup(name);
        RaftNode raftNode = raftGroup.localNode();
        String leader = raftNode.getLeader();
        return Context.newBuilder().setName(raftNode.groupId())
                      .addAllMembers(raftGroup.localNode().currentGroupMembers()
                                              .stream()
                                              .map(n -> ContextMember.newBuilder()
                                                                     .setNodeId(n.getNodeId())
                                                                     .setNodeName(n.getNodeName())
                                                                     .setState(n.getNodeId()
                                                                                .equals(leader) ? State.LEADER : State.VOTING)
                                                                     .build())
                                              .collect(Collectors.toList())).build();
    }

    @Override
    public void stepDown(String context) {
        RaftNode raftNode = grpcRaftController.getRaftNode(context);
        if (raftNode != null ) {
            raftNode.stepdown();
        }
    }

    @Override
    public CompletableFuture<Void> updateApplication(ContextApplication application) {
        RaftNode raftNode = grpcRaftController.getRaftNode(application.getContext());
        return raftNode.appendEntry(ContextApplication.class.getName(), application.toByteArray());
    }

    @Override
    public CompletableFuture<Void> updateLoadBalancingStrategy(String context,
                                                               LoadBalanceStrategy loadBalancingStrategy) {
        RaftNode raftNode = grpcRaftController.getRaftNode(context);
        return raftNode.appendEntry(LoadBalanceStrategy.class.getName(), loadBalancingStrategy.toByteArray());
    }

    @Override
    public CompletableFuture<Void> updateProcessorLoadBalancing(String context,
                                                                ProcessorLBStrategy processorLBStrategy) {
        return appendEntry(context, ProcessorLBStrategy.class.getName(), processorLBStrategy.toByteArray());
    }

    @Override
    public CompletableFuture<Void> appendEntry(String context, String name, byte[] bytes) {
        RaftNode raftNode = grpcRaftController.getRaftNode(context);
        if (raftNode == null) {
            CompletableFuture<Void> error = new CompletableFuture<>();
            error.completeExceptionally(new MessagingPlatformException(ErrorCode.OTHER,
                                                                       "Cannot find node for context"));
            return error;
        }
        return raftNode.appendEntry(name, bytes);
    }

    @Override
    public CompletableFuture<Void> deleteLoadBalancingStrategy(String context,
                                                               LoadBalanceStrategy loadBalancingStrategy) {
        RaftNode raftNode = grpcRaftController.getRaftNode(context);
        return raftNode.appendEntry(DELETE_LOAD_BALANCING_STRATEGY, loadBalancingStrategy.toByteArray());
    }

    @Override
    public CompletableFuture<Void> deleteContext(String context) {
        RaftNode raftNode = null;
        try {
            raftNode = grpcRaftController.getRaftNode(context);
        } catch(MessagingPlatformException ex) {
            return CompletableFuture.completedFuture(null);
        }
        return raftNode.removeGroup().thenAccept(r -> grpcRaftController.delete(context));
    }
}
