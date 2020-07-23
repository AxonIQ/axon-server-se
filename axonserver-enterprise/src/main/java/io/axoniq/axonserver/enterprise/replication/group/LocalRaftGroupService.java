package io.axoniq.axonserver.enterprise.replication.group;

import io.axoniq.axonserver.cluster.RaftGroup;
import io.axoniq.axonserver.cluster.RaftNode;
import io.axoniq.axonserver.cluster.util.AxonThreadFactory;
import io.axoniq.axonserver.enterprise.replication.GrpcRaftController;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.ReplicationGroupMemberConverter;
import io.axoniq.axonserver.grpc.cluster.ConfigChangeResult;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.internal.NodeInfo;
import io.axoniq.axonserver.grpc.internal.NodeInfoWithLabel;
import io.axoniq.axonserver.grpc.internal.ReplicationGroup;
import io.axoniq.axonserver.grpc.internal.ReplicationGroupConfiguration;
import io.axoniq.axonserver.grpc.internal.ReplicationGroupMember;
import io.axoniq.axonserver.grpc.internal.ReplicationGroupUpdateConfirmation;
import io.axoniq.axonserver.grpc.internal.State;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static io.axoniq.axonserver.enterprise.CompetableFutureUtils.getFuture;
import static java.util.stream.Collectors.toSet;

/**
 * @author Marc Gathier
 */
@Component
public class LocalRaftGroupService implements RaftGroupService {

    private final Logger logger = LoggerFactory.getLogger(LocalRaftGroupService.class);
    private final ExecutorService asyncPool = Executors.newCachedThreadPool(new AxonThreadFactory("raft-async-"));

    private final GrpcRaftController grpcRaftController;

    public LocalRaftGroupService(GrpcRaftController grpcRaftController) {
        this.grpcRaftController = grpcRaftController;
    }

    @Override
    public CompletableFuture<ReplicationGroupUpdateConfirmation> addServer(String replicationGroup, Node node) {
        CompletableFuture<ReplicationGroupUpdateConfirmation> result = new CompletableFuture<>();
        RaftNode raftNode = grpcRaftController.getRaftNode(replicationGroup);
        Set<String> members = raftNode.currentGroupMembers().stream().map(Node::getNodeName).collect(toSet());
        if (members.contains(node.getNodeName())) {
            result.complete(ReplicationGroupUpdateConfirmation.newBuilder()
                                                              .setSuccess(true)
                                                              .addAllMembers(raftNode.currentGroupMembers()
                                                                                     .stream()
                                                                                     .map(ReplicationGroupMemberConverter::asContextMember)
                                                                                     .collect(Collectors.toList()))
                                                              .build());
        } else {
            asyncPool.submit(() -> addNodeAsync(replicationGroup, node, result, raftNode));
        }
        return result;
    }

    private void addNodeAsync(String context, Node node, CompletableFuture<ReplicationGroupUpdateConfirmation> result,
                              RaftNode raftNode) {
        raftNode.addNode(node).whenComplete((configChangeResult, throwable) -> {
            if (throwable != null) {
                logger.error("{}: Exception while adding node {}", context, node.getNodeName(), throwable);
            }
            ReplicationGroupUpdateConfirmation contextUpdateConfirmation = createReplicationGroupUpdateConfirmation(
                    raftNode,
                    configChangeResult,
                    throwable);

            result.complete(contextUpdateConfirmation);
        });
    }

    @Override
    public CompletableFuture<Void> getStatus(Consumer<ReplicationGroup> contextConsumer) {
        grpcRaftController.raftGroups().forEach(name -> contextConsumer.accept(getStatus(name)));
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<ReplicationGroupConfiguration> configuration(String context) {
        CompletableFuture<ReplicationGroupConfiguration> completableFuture = new CompletableFuture<>();
        ReplicationGroupConfiguration configuration = buildConfiguration(context);
        if (configuration == null) {
            completableFuture.completeExceptionally(new RuntimeException("Context not found: " + context));
        } else {
            completableFuture.complete(configuration);
        }
        return completableFuture;
    }

    @Nullable
    private ReplicationGroupConfiguration buildConfiguration(String replicationGroup) {
        RaftGroup raftGroup = grpcRaftController.getRaftGroup(replicationGroup);
        if (raftGroup == null) {
            return null;
        }
        RaftNode raftNode = raftGroup.localNode();
        ReplicationGroupConfiguration.Builder builder = ReplicationGroupConfiguration
                .newBuilder()
                .setReplicationGroupName(replicationGroup)
                .setPending(raftNode.isCurrentConfigurationPending());
        for (Node member : raftNode.currentGroupMembers()) {
            builder.addNodes(NodeInfoWithLabel
                                     .newBuilder()
                                     .setLabel(member.getNodeId())
                                     .setRole(member.getRole())
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
    public CompletableFuture<ReplicationGroupUpdateConfirmation> deleteServer(String context, String node) {
        CompletableFuture<ReplicationGroupUpdateConfirmation> result = new CompletableFuture<>();
        RaftNode raftNode = grpcRaftController.getRaftNode(context);
        try {
            raftNode.removeNode(node).whenComplete(((configChangeResult, throwable) -> {
                if (throwable != null) {
                    logger.error("{}: Exception while deleting node {}", context, node, throwable);
                }
                try {
                    ReplicationGroupUpdateConfirmation contextUpdateConfirmation = createReplicationGroupUpdateConfirmation(
                            raftNode,
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
    private ReplicationGroupUpdateConfirmation createReplicationGroupUpdateConfirmation(RaftNode raftNode,
                                                                                        ConfigChangeResult configChangeResult,
                                                                                        Throwable throwable) {
        ReplicationGroupUpdateConfirmation.Builder builder = ReplicationGroupUpdateConfirmation.newBuilder();
        raftNode.currentGroupMembers().forEach(n -> builder
                .addMembers(ReplicationGroupMemberConverter.asContextMember(n)));
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
                builder.setMessage(configChangeResult.getFailure().getError().getMessage());
            } else {
                builder.setSuccess(true);
                builder.setPending(false);
            }
        }
        return builder.build();
    }

    @Override
    public CompletableFuture<ReplicationGroupConfiguration> initReplicationGroup(String replicationGroup,
                                                                                 List<Node> raftNodes) {
        try {
            RaftGroup raftGroup = grpcRaftController.initRaftGroup(replicationGroup,
                                                                   grpcRaftController.getMyLabel(raftNodes),
                                                                   grpcRaftController.getMyName());
            RaftNode leader = grpcRaftController.waitForLeader(raftGroup);
            raftNodes.forEach(n -> getFuture(leader.addNode(n)));

            return CompletableFuture.completedFuture(buildConfiguration(replicationGroup));
        } catch (Exception ex) {
            logger.error("{}: create replication group failed", replicationGroup, ex);
            CompletableFuture<ReplicationGroupConfiguration> result = new CompletableFuture<>();
            result.completeExceptionally(ex);
            return result;
        }
    }


    public ReplicationGroup getStatus(String name) {
        RaftGroup raftGroup = grpcRaftController.getRaftGroup(name);
        RaftNode raftNode = raftGroup.localNode();
        String leader = raftNode.getLeader();
        return ReplicationGroup.newBuilder().setName(raftNode.groupId())
                               .addAllMembers(raftNode.currentGroupMembers()
                                                      .stream()
                                                      .map(n -> ReplicationGroupMember.newBuilder()
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
    @Transactional
    public CompletableFuture<Void> deleteReplicationGroup(String replicationGroup, boolean preserveEventStore) {
        RaftNode raftNode = null;
        try {
            raftNode = grpcRaftController.getRaftNode(replicationGroup);
        } catch (MessagingPlatformException ex) {
            return CompletableFuture.completedFuture(null);
        }
        return raftNode.removeGroup().thenAccept(r -> grpcRaftController.delete(replicationGroup, preserveEventStore));
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

    @Override
    public CompletableFuture<Void> prepareDeleteNodeFromReplicationGroup(String context, String node) {
        grpcRaftController.prepareDeleteNodeFromReplicationGroup(context, node);
        return CompletableFuture.completedFuture(null);
    }
}
