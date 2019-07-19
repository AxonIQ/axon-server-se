package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.cluster.RaftGroup;
import io.axoniq.axonserver.cluster.RaftNode;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.cluster.ConfigChangeResult;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.internal.Context;
import io.axoniq.axonserver.grpc.internal.ContextApplication;
import io.axoniq.axonserver.grpc.internal.ContextConfiguration;
import io.axoniq.axonserver.grpc.internal.ContextMember;
import io.axoniq.axonserver.grpc.internal.ContextUpdateConfirmation;
import io.axoniq.axonserver.grpc.internal.LoadBalanceStrategy;
import io.axoniq.axonserver.grpc.internal.NodeInfo;
import io.axoniq.axonserver.grpc.internal.NodeInfoWithLabel;
import io.axoniq.axonserver.grpc.internal.ProcessorLBStrategy;
import io.axoniq.axonserver.grpc.internal.State;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static io.axoniq.axonserver.enterprise.CompetableFutureUtils.getFuture;
import static io.axoniq.axonserver.enterprise.logconsumer.DeleteLoadBalancingStrategyConsumer.DELETE_LOAD_BALANCING_STRATEGY;
import static java.util.stream.Collectors.toSet;

/**
 * @author Marc Gathier
 */
@Component
public class LocalRaftGroupService implements RaftGroupService {

    private final Logger logger = LoggerFactory.getLogger(LocalRaftGroupService.class);
    private final ExecutorService asyncPool = Executors.newCachedThreadPool();

    private GrpcRaftController grpcRaftController;

    public LocalRaftGroupService(GrpcRaftController grpcRaftController) {
        this.grpcRaftController = grpcRaftController;
    }

    @Override
    public CompletableFuture<ContextUpdateConfirmation> addNodeToContext(String context, Node node) {
        CompletableFuture<ContextUpdateConfirmation> result = new CompletableFuture<>();
        RaftNode raftNode = grpcRaftController.getRaftNode(context);
        Set<String> members = raftNode.currentGroupMembers().stream().map(Node::getNodeName).collect(toSet());
        if (members.contains(node.getNodeName())) {
//            MessagingPlatformException ex = new MessagingPlatformException(ErrorCode.ALREADY_MEMBER_OF_CLUSTER,
//                                                                           "Node is already part of this context.");
            result.complete(ContextUpdateConfirmation.newBuilder()
                                                     .setSuccess(true)
                                                     .addAllMembers(raftNode.currentGroupMembers()
                                                                            .stream()
                                                                            .map(n -> ContextMember.newBuilder()
                                                                                                   .setNodeName(n.getNodeName())
                                                                                                   .setNodeId(n.getNodeId())
                                                                                                   .setPort(n.getPort())
                                                                                                   .setHost(n.getHost())
                                                                                                   .build())
                                                                            .collect(Collectors.toList()))
                                                     .build());
        } else {
            asyncPool.submit(() -> addNodeAsync(context, node, result, raftNode));
        }
        return result;
    }

    private void addNodeAsync(String context, Node node, CompletableFuture<ContextUpdateConfirmation> result,
                              RaftNode raftNode) {
        raftNode.addNode(node).whenComplete((configChangeResult, throwable) -> {
            if (throwable != null) {
                logger.error("{}: Exception while adding node {}", context, node.getNodeName(), throwable);
            }
            ContextUpdateConfirmation contextUpdateConfirmation = createContextUpdateConfirmation(raftNode,
                                                                                                  configChangeResult,
                                                                                                  throwable);

            result.complete(contextUpdateConfirmation);
        });
    }

    @Override
    public CompletableFuture<Void> getStatus(Consumer<Context> contextConsumer) {
        grpcRaftController.raftGroups().forEach(name -> contextConsumer.accept(getStatus(name)));
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<ContextConfiguration> configuration(String context) {
        CompletableFuture<ContextConfiguration> completableFuture = new CompletableFuture<>();
        ContextConfiguration configuration = buildConfiguration(context);
        if (configuration == null) {
            completableFuture.completeExceptionally(new RuntimeException("Context not found: " + context));
        } else {
            completableFuture.complete(configuration);
        }
        return completableFuture;
    }

    @Nullable
    private ContextConfiguration buildConfiguration(String context) {
        RaftGroup raftGroup = grpcRaftController.getRaftGroup(context);
        if (raftGroup == null) {
            return null;
        }
        RaftNode raftNode = raftGroup.localNode();
        ContextConfiguration.Builder builder = ContextConfiguration
                .newBuilder()
                .setContext(context)
                .setPending(raftNode.isCurrentConfigurationPending());
        for (Node member : raftNode.currentGroupMembers()) {
            builder.addNodes(NodeInfoWithLabel
                                     .newBuilder()
                                     .setLabel(member.getNodeId())
                                     .setNode(NodeInfo
                                                      .newBuilder()
                                                      .setNodeName(member.getNodeName())
                                                      .setInternalHostName(member.getHost())
                                                      .setGrpcInternalPort(member.getPort())
                                                      .build())
                                     .build());
        }
        return builder.build();
    }

    @Override
    public CompletableFuture<ContextUpdateConfirmation> deleteNode(String context, String node) {
        CompletableFuture<ContextUpdateConfirmation> result = new CompletableFuture<>();
        RaftNode raftNode = grpcRaftController.getRaftNode(context);
        try {
            raftNode.removeNode(node).whenComplete(((configChangeResult, throwable) -> {
                if (throwable != null) {
                    logger.error("{}: Exception while deleting node {}", context, node, throwable);
                }
                try {
                    ContextUpdateConfirmation contextUpdateConfirmation = createContextUpdateConfirmation(raftNode,
                                                                                                          configChangeResult,
                                                                                                          throwable);

                    result.complete(contextUpdateConfirmation);
                } catch (Exception ex) {
                    logger.error("{}: Exception while deleting node {}", context, node, ex);
                    result.completeExceptionally(ex);
                }
            }));
        } catch (Exception ex) {
            logger.error("{}: Exception while deleting node {}", context, node, ex);
            result.completeExceptionally(ex);
        }
        return result;
    }

    @NotNull
    private ContextUpdateConfirmation createContextUpdateConfirmation(RaftNode raftNode,
                                                                      ConfigChangeResult configChangeResult,
                                                                      Throwable throwable) {
        ContextUpdateConfirmation.Builder builder = ContextUpdateConfirmation.newBuilder();

        raftNode.currentGroupMembers().forEach(n -> builder.addMembers(ContextMember.newBuilder()
                                                                                    .setNodeId(n.getNodeId())
                                                                                    .setNodeName(n.getNodeName())
                                                                                    .setPort(n.getPort())
                                                                                    .setHost(n.getHost())));

        builder.setPending(raftNode.isCurrentConfigurationPending());

        if (throwable != null) {
            builder.setSuccess(false);
            builder.setMessage(throwable.getMessage());
        } else {
            if (configChangeResult.hasFailure()) {
                logger.error("{}: Exception while changing configuration: {}",
                             raftNode.groupId(),
                             configChangeResult.getFailure());
                builder.setSuccess(false);
                builder.setMessage(configChangeResult.getFailure().toString());
            } else {
                builder.setSuccess(true);
                builder.setPending(false);
            }
        }
        return builder.build();
    }

    @Override
    public CompletableFuture<ContextConfiguration> initContext(String context, List<Node> raftNodes) {
        try {
            RaftGroup raftGroup = grpcRaftController.initRaftGroup(context, grpcRaftController.getMyLabel(raftNodes),
                                                                   grpcRaftController.getMyName());
            RaftNode leader = grpcRaftController.waitForLeader(raftGroup);
            raftNodes.forEach(n -> getFuture(leader.addNode(n)));

            return CompletableFuture.completedFuture(buildConfiguration(context));
        } catch (Exception ex) {
            logger.error("{}: create context failed", context, ex);
            CompletableFuture<ContextConfiguration> result = new CompletableFuture<>();
            result.completeExceptionally(ex);
            return result;
        }
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
        if (raftNode != null) {
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
        } catch (MessagingPlatformException ex) {
            return CompletableFuture.completedFuture(null);
        }
        return raftNode.removeGroup().thenAccept(r -> grpcRaftController.delete(context));
    }

    @Override
    public CompletableFuture<Void> transferLeadership(String context) {
        try {
            RaftNode raftNode = grpcRaftController.getRaftNode(context);
            return raftNode.transferLeadership();
        } catch (MessagingPlatformException ex) {
            return CompletableFuture.completedFuture(null);
        }
    }
}
