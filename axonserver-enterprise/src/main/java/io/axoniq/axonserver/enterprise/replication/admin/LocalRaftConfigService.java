package io.axoniq.axonserver.enterprise.replication.admin;

import com.google.protobuf.ByteString;
import io.axoniq.axonserver.access.application.AdminApplication;
import io.axoniq.axonserver.access.application.AdminApplicationContext;
import io.axoniq.axonserver.access.application.AdminApplicationContextRole;
import io.axoniq.axonserver.access.application.AdminApplicationController;
import io.axoniq.axonserver.access.application.ApplicationNotFoundException;
import io.axoniq.axonserver.access.jpa.UserRole;
import io.axoniq.axonserver.access.user.UserController;
import io.axoniq.axonserver.cluster.RaftGroup;
import io.axoniq.axonserver.cluster.RaftNode;
import io.axoniq.axonserver.cluster.util.RoleUtils;
import io.axoniq.axonserver.config.FeatureChecker;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.CompetableFutureUtils;
import io.axoniq.axonserver.enterprise.ContextEvents;
import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import io.axoniq.axonserver.enterprise.component.processor.balancing.jpa.ReplicationGroupProcessorLoadBalancing;
import io.axoniq.axonserver.enterprise.context.AdminContextController;
import io.axoniq.axonserver.enterprise.context.ContextNameValidation;
import io.axoniq.axonserver.enterprise.jpa.AdminContext;
import io.axoniq.axonserver.enterprise.jpa.AdminReplicationGroup;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.enterprise.replication.GrpcRaftController;
import io.axoniq.axonserver.enterprise.replication.group.RaftGroupServiceFactory;
import io.axoniq.axonserver.enterprise.replication.logconsumer.AdminContextConsumer;
import io.axoniq.axonserver.enterprise.replication.logconsumer.AdminDeleteContextConsumer;
import io.axoniq.axonserver.enterprise.replication.logconsumer.AdminNodeConsumer;
import io.axoniq.axonserver.enterprise.storage.ContextPropertyDefinition;
import io.axoniq.axonserver.enterprise.storage.file.EmbeddedDBPropertiesProvider;
import io.axoniq.axonserver.enterprise.taskscheduler.TaskPublisher;
import io.axoniq.axonserver.enterprise.taskscheduler.task.NodeContext;
import io.axoniq.axonserver.enterprise.taskscheduler.task.PrepareDeleteNodeFromContextTask;
import io.axoniq.axonserver.enterprise.taskscheduler.task.UnregisterNodeTask;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.ApplicationProtoConverter;
import io.axoniq.axonserver.grpc.UserProtoConverter;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.cluster.Role;
import io.axoniq.axonserver.grpc.internal.Application;
import io.axoniq.axonserver.grpc.internal.ApplicationContextRole;
import io.axoniq.axonserver.grpc.internal.ContextApplication;
import io.axoniq.axonserver.grpc.internal.ContextRole;
import io.axoniq.axonserver.grpc.internal.ContextUser;
import io.axoniq.axonserver.grpc.internal.DeleteContextRequest;
import io.axoniq.axonserver.grpc.internal.DeleteNode;
import io.axoniq.axonserver.grpc.internal.NodeInfo;
import io.axoniq.axonserver.grpc.internal.NodeInfoWithLabel;
import io.axoniq.axonserver.grpc.internal.ProcessorLBStrategy;
import io.axoniq.axonserver.grpc.internal.ReplicationGroupConfiguration;
import io.axoniq.axonserver.grpc.internal.ReplicationGroupContext;
import io.axoniq.axonserver.grpc.internal.ReplicationGroupMember;
import io.axoniq.axonserver.grpc.internal.ReplicationGroupUpdateConfirmation;
import io.axoniq.axonserver.grpc.internal.UpdateLicense;
import io.axoniq.axonserver.grpc.internal.User;
import io.axoniq.axonserver.grpc.internal.UserContextRole;
import io.axoniq.axonserver.licensing.Feature;
import io.axoniq.axonserver.licensing.LicenseManager;
import io.axoniq.axonserver.taskscheduler.TransientException;
import io.axoniq.axonserver.util.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.EventListener;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static io.axoniq.axonserver.RaftAdminGroup.getAdmin;
import static io.axoniq.axonserver.RaftAdminGroup.isAdmin;
import static io.axoniq.axonserver.enterprise.CompetableFutureUtils.getFuture;
import static io.axoniq.axonserver.enterprise.replication.logconsumer.AdminDeleteApplicationConsumer.DELETE_APPLICATION;
import static io.axoniq.axonserver.enterprise.replication.logconsumer.AdminDeleteUserConsumer.DELETE_USER;
import static io.axoniq.axonserver.rest.ClusterRestController.CONTEXT_NONE;
import static io.axoniq.axonserver.util.StringUtils.getOrDefault;
import static io.axoniq.axonserver.util.StringUtils.isEmpty;

/**
 * Service to orchestrate configuration changes. This service is executed on the leader of the _admin context.
 *
 * @author Marc Gathier
 */
@Component
public class LocalRaftConfigService implements RaftConfigService {

    private static final long MAX_PENDING_TIME = TimeUnit.SECONDS.toMillis(30);
    private final GrpcRaftController grpcRaftController;
    private final AdminContextController contextController;
    private final ClusterController clusterController;
    private final RaftGroupServiceFactory raftGroupServiceFactory;
    private final AdminApplicationController applicationController;
    private final UserController userController;
    private final MessagingPlatformConfiguration messagingPlatformConfiguration;
    private final TaskPublisher taskPublisher;
    private final Predicate<String> contextNameValidation = new ContextNameValidation();
    private final CopyOnWriteArraySet<String> replicationGroupsInProgress = new CopyOnWriteArraySet<>();
    private final AdminReplicationGroupController adminReplicationGroupController;
    private final LicenseManager licenseManager;
    private final FeatureChecker limits;
    private final ApplicationEventPublisher applicationEventPublisher;
    private final Logger logger = LoggerFactory.getLogger(LocalRaftConfigService.class);


    public LocalRaftConfigService(GrpcRaftController grpcRaftController,
                                  AdminContextController contextController,
                                  ClusterController clusterController,
                                  RaftGroupServiceFactory raftGroupServiceFactory,
                                  AdminApplicationController applicationController,
                                  UserController userController,
                                  MessagingPlatformConfiguration messagingPlatformConfiguration,
                                  TaskPublisher taskPublisher,
                                  LicenseManager licenseManager,
                                  FeatureChecker limits,
                                  AdminReplicationGroupController adminReplicationGroupController,
                                  ApplicationEventPublisher applicationEventPublisher) {
        this.grpcRaftController = grpcRaftController;
        this.contextController = contextController;
        this.clusterController = clusterController;
        this.raftGroupServiceFactory = raftGroupServiceFactory;
        this.applicationController = applicationController;
        this.userController = userController;
        this.messagingPlatformConfiguration = messagingPlatformConfiguration;
        this.taskPublisher = taskPublisher;
        this.licenseManager = licenseManager;
        this.limits = limits;
        this.adminReplicationGroupController = adminReplicationGroupController;
        this.applicationEventPublisher = applicationEventPublisher;
    }

    @Override
    public void createReplicationGroup(String name, Collection<ReplicationGroupMember> members) {
        if (!Feature.MULTI_CONTEXT.enabled(limits)) {
            throw new MessagingPlatformException(ErrorCode.CONTEXT_CREATION_NOT_ALLOWED,
                                                 "License does not allow creating replication groups");
        }

        if (!replicationGroupsInProgress.add(name)) {
            throw new UnsupportedOperationException("The creation of the replication group is already in progress.");
        }
        adminReplicationGroupController.findByName(name).ifPresent(group -> {
            replicationGroupsInProgress.remove(name);
            throw new MessagingPlatformException(ErrorCode.CONTEXT_EXISTS,
                                                 String.format("Replication group %s already exists", name));
        });

        Set<String> unknownNodes = members
                .stream()
                .filter(m -> clusterController.getNode(m.getNodeName()) == null)
                .map(ReplicationGroupMember::getNodeName)
                .collect(Collectors.toSet());
        if (!unknownNodes.isEmpty()) {
            replicationGroupsInProgress.remove(name);
            throw new MessagingPlatformException(ErrorCode.NO_SUCH_NODE,
                                                 String.format("Node(s) %s not found", unknownNodes));
        }

        List<Node> raftNodes = new ArrayList<>();
        AtomicReference<Node> target = new AtomicReference<>();
        members.forEach(n -> {
            ClusterNode clusterNode = clusterController.getNode(n.getNodeName());
            String nodeLabel = generateNodeLabel(n.getNodeName());
            Node raftNode = createNode(clusterNode, nodeLabel, n.getRole());
            if (target.get() == null && Role.PRIMARY.equals(raftNode.getRole())) {
                target.set(raftNode);
            }
            raftNodes.add(raftNode);
        });

        if (target.get() == null) {
            throw new MessagingPlatformException(ErrorCode.CONTEXT_CREATION_NOT_ALLOWED,
                                                 "No primary nodes provided");
        }

        try {
            getFuture(
                    raftGroupServiceFactory.getRaftGroupServiceForNode(target.get().getNodeName()).initReplicationGroup(
                            name,
                            raftNodes)
                                           .thenAccept(replicationGroupConfiguration -> {
                                               ReplicationGroupConfiguration completed =
                                                       ReplicationGroupConfiguration.newBuilder(
                                                               replicationGroupConfiguration)
                                                                                    .setPending(false)
                                                                                    .build();
                                               appendToAdmin(ReplicationGroupConfiguration.class.getName(),
                                                             completed.toByteArray());
                                           }).whenComplete((success, error) -> {
                        replicationGroupsInProgress.remove(name);
                        if (error != null) {
                            deleteReplicationGroup(name, false);
                        }
                    }));
        } catch (RuntimeException runtimeException) {
            replicationGroupsInProgress.remove(name);
            throw runtimeException;
        }
    }

    @Override
    public CompletableFuture<Void> addNodeToReplicationGroup(String replicationGroupName, String node, Role role) {
        logger.info("Add node request invoked for node: {} - and context: {}", node, replicationGroupName);
        if (!replicationGroupsInProgress.add(replicationGroupName)) {
            throw new MessagingPlatformException(ErrorCode.CONTEXT_UPDATE_IN_PROGRESS,
                                                 replicationGroupName + ": pending update");
        }

        AdminReplicationGroup replicationGroup = adminReplicationGroupController.findByName(replicationGroupName)
                                                                                .orElseThrow(() -> {
                                                                                    replicationGroupsInProgress.remove(
                                                                                            replicationGroupName);
                                                                                    return new MessagingPlatformException(
                                                                                            ErrorCode.CONTEXT_NOT_FOUND,
                                                                                            String.format(
                                                                                                    "Replication group %s not found",
                                                                                                    replicationGroupName));
                                                                                });

        if (replicationGroup.isChangePending()
                && replicationGroup.getPendingSince().getTime() > System.currentTimeMillis() - MAX_PENDING_TIME) {
            replicationGroupsInProgress.remove(replicationGroupName);
            throw new MessagingPlatformException(ErrorCode.CONTEXT_UPDATE_IN_PROGRESS,
                                                 replicationGroupName + ": pending update");
        }
        ClusterNode clusterNode = clusterController.getNode(node);
        if (clusterNode == null) {
            replicationGroupsInProgress.remove(replicationGroupName);
            throw new MessagingPlatformException(ErrorCode.NO_SUCH_NODE, String.format("Node %s not found", node));
        }
        if (clusterNode.getReplicatorGroupNames().contains(replicationGroupName)) {
            logger.info("{} already contains node: {}", replicationGroupName, node);
            replicationGroupsInProgress.remove(replicationGroupName);
            return null;
        }
        ReplicationGroupConfiguration oldConfiguration = createReplicationGroupConfigurationBuilder(replicationGroup)
                .build();
        try {
            String nodeLabel = generateNodeLabel(node);
            Node raftNode = createNode(clusterNode, nodeLabel, role);

            ReplicationGroupConfiguration contextConfiguration =
                    ReplicationGroupConfiguration.newBuilder(oldConfiguration).setPending(true)
                                                 .build();

            appendToAdmin(ReplicationGroupConfiguration.class.getName(), contextConfiguration.toByteArray());

            return raftGroupServiceFactory.getRaftGroupService(replicationGroupName)
                                          .addServer(replicationGroupName, raftNode)
                                          .thenAccept(result -> handleReplicationGroupUpdateResult(replicationGroupName,
                                                                                                   result))
                                          .exceptionally(e -> {
                                              raftGroupServiceFactory.getRaftGroupServiceForNode(raftNode.getNodeName())
                                                                     .deleteReplicationGroup(replicationGroupName,
                                                                                             false);
                                              resetAdminConfiguration(oldConfiguration,
                                                                      "Failed to add node: " + node,
                                                                      e);
                                              throw new MessagingPlatformException(ErrorCode.CONTEXT_UPDATE_IN_PROGRESS,
                                                                                   e.getMessage());
                                          });
        } catch (RuntimeException throwable) {
            resetAdminConfiguration(oldConfiguration, "Failed to add node: " + node, throwable);
            replicationGroupsInProgress.remove(replicationGroupName);
            throw throwable;
        } finally {
            replicationGroupsInProgress.remove(replicationGroupName);
        }
    }

    private void handleReplicationGroupUpdateResult(String context,
                                                    ReplicationGroupUpdateConfirmation result) {
        replicationGroupsInProgress.remove(context);
        if (!result.getSuccess()) {
            logger.error("{}: {}", context, result.getMessage());
            throw new MessagingPlatformException(ErrorCode.CONTEXT_UPDATE_IN_PROGRESS, result.getMessage());
        }
        ReplicationGroupConfiguration updatedConfiguration = createReplicationGroupConfiguration(context, result);
        try {
            sendToAdmin(updatedConfiguration.getClass().getName(), updatedConfiguration.toByteArray());
        } catch (Exception exception) {
            logger.warn("{}: Error sending updated configuration to admin {}", context, exception.getMessage());
        }
    }

    private Void resetAdminConfiguration(ReplicationGroupConfiguration oldConfiguration, String message,
                                         Throwable reason) {
        if (oldConfiguration == null) {
            logger.error("{}", message, reason);
            return null;
        }
        logger.error("{}: {}", oldConfiguration.getReplicationGroupName(), message, reason);
        try {
            appendToAdmin(oldConfiguration.getClass().getName(), oldConfiguration.toByteArray());
        } catch (Exception adminException) {
            logger.debug("{}: Error while restoring old configuration in admin {}",
                         oldConfiguration.getReplicationGroupName(),
                         adminException.getMessage());
        }
        return null;
    }

    private String generateNodeLabel(String node) {
        return node + "-" + UUID.randomUUID();
    }

    private Node createNode(ClusterNode clusterNode, String nodeLabel, Role role) {
        return Node.newBuilder().setNodeId(nodeLabel)
                   .setHost(clusterNode.getInternalHostName())
                   .setPort(clusterNode.getGrpcInternalPort())
                   .setNodeName(clusterNode.getName())
                   .setRole(RoleUtils.getOrDefault(role))
                   .build();
    }

    @Override
    public void deleteReplicationGroup(String replicationGroupName, boolean preserveEventStore) {
        logger.info("Delete replication group: {}", replicationGroupName);
        if (isAdmin(replicationGroupName)) {
            throw new MessagingPlatformException(ErrorCode.CANNOT_DELETE_INTERNAL_CONTEXT,
                                                 String.format("Deletion of internal replication group %s not allowed",
                                                               replicationGroupName));
        }

        AdminReplicationGroup adminReplicationGroup = adminReplicationGroupController.findByName(replicationGroupName)
                                                                                     .orElse(null);
        if (adminReplicationGroup == null) {
            logger.warn("Could not find context {} in admin tables, sending deleteContext to all nodes",
                        replicationGroupName);
            clusterController.remoteNodeNames().forEach(node -> raftGroupServiceFactory.getRaftGroupServiceForNode(node)
                                                                                       .deleteReplicationGroup(
                                                                                               replicationGroupName,
                                                                                               preserveEventStore));
            raftGroupServiceFactory.getRaftGroupServiceForNode(this.messagingPlatformConfiguration.getName())
                                   .deleteReplicationGroup(replicationGroupName, preserveEventStore);
            replicationGroupsInProgress.remove(replicationGroupName);
            return;
        }
        Collection<String> nodeNames = adminReplicationGroup.getMemberNames();
        @SuppressWarnings("unchecked")
        CompletableFuture<Void>[] workers = new CompletableFuture[nodeNames.size()];

        ReplicationGroupConfiguration contextConfiguration = createReplicationGroupConfigurationBuilder(
                adminReplicationGroup)
                .setPending(true)
                .build();

        appendToAdmin(ReplicationGroupConfiguration.class.getName(), contextConfiguration.toByteArray());

        int nodeIdx = 0;
        Iterable<String> nodes = new HashSet<>(nodeNames);
        for (String nodeName : nodes) {
            workers[nodeIdx] = raftGroupServiceFactory.getRaftGroupServiceForNode(nodeName).deleteReplicationGroup(
                    replicationGroupName,
                    preserveEventStore);
            workers[nodeIdx].thenAccept(r -> nodeNames.remove(nodeName));
            nodeIdx++;
        }

        CompletableFuture.allOf(workers).whenComplete((result, exception) -> {
            ReplicationGroupConfiguration.Builder updatedContextConfigurationBuilder =
                    ReplicationGroupConfiguration.newBuilder()
                                                 .setReplicationGroupName(replicationGroupName);
            if (exception != null) {
                logger.warn("{}: Could not delete context from {}",
                            replicationGroupName,
                            String.join(",", nodeNames),
                            exception);

                adminReplicationGroup.getMembers().stream().filter(c -> nodeNames
                        .contains(c.getClusterNode().getName()))
                                     .forEach(c -> updatedContextConfigurationBuilder
                                             .addNodes(NodeInfoWithLabel.newBuilder()
                                                                        .setLabel(c.getClusterNodeLabel())
                                                                        .setNode(c.getClusterNode()
                                                                                  .toNodeInfo())));
            }
            try {
                appendToAdmin(ReplicationGroupConfiguration.class.getName(),
                              updatedContextConfigurationBuilder.build().toByteArray());
            } catch (Exception second) {
                logger.debug("{}: Error while updating configuration {}", replicationGroupName, second.getMessage());
            }
            replicationGroupsInProgress.remove(replicationGroupName);
        });
    }


    @Override
    public void deleteNodeFromReplicationGroup(String replicationGroup, String node) {
        logger.info("Delete node from context invoked for context: {} - and node: {}", replicationGroup, node);
        AdminReplicationGroup adminReplicationGroup =
                adminReplicationGroupController.findByName(replicationGroup)
                                               .orElseThrow(() ->
                                                                    new MessagingPlatformException(ErrorCode.CONTEXT_NOT_FOUND,
                                                                                                   String.format(
                                                                                                           "Replication group %s not found",
                                                                                                           replicationGroup)));


        String nodeLabel = adminReplicationGroup.getNodeLabel(node).orElseThrow(() ->
                                                                                        new MessagingPlatformException(
                                                                                                ErrorCode.NO_SUCH_NODE,
                                                                                                String.format(
                                                                                                        "Node %s not found in replicationGroup %s",
                                                                                                        node,
                                                                                                        replicationGroup)));


        if (adminReplicationGroup.getMembers().size() == 1) {
            throw new MessagingPlatformException(ErrorCode.CANNOT_REMOVE_LAST_NODE,
                                                 String.format(
                                                         "Node %s is last node in replication group %s, to delete the replication group use unregister context",
                                                         node,
                                                         replicationGroup));
        }

        CompetableFutureUtils.getFuture(removeNodeFromReplicationGroup(adminReplicationGroup, node, nodeLabel),
                                        1,
                                        TimeUnit.MINUTES);
    }

    private CompletableFuture<Void> removeNodeFromReplicationGroup(AdminReplicationGroup context,
                                                                   String node, String nodeLabel) {
        CompletableFuture<Void> removeDone = new CompletableFuture<>();
        ReplicationGroupConfiguration oldConfiguration = createReplicationGroupConfigurationBuilder(context)
                .build();
        try {
            ReplicationGroupConfiguration contextConfiguration = ReplicationGroupConfiguration.newBuilder(
                    oldConfiguration)
                                                                                              .setPending(true)
                                                                                              .build();
            appendToAdmin(ReplicationGroupConfiguration.class.getName(), contextConfiguration.toByteArray());

            transferLeader(context, node);

            raftGroupServiceFactory.getRaftGroupService(context.getName())
                                   .deleteServer(context.getName(), nodeLabel)
                                   .thenAccept(result -> handleReplicationGroupUpdateResult(context.getName(), result))
                                   .exceptionally(e -> resetAdminConfiguration(oldConfiguration,
                                                                               "Failed to delete node " + node,
                                                                               e))
                                   .thenAccept(e -> removeDone.complete(null));
        } catch (Exception ex) {
            resetAdminConfiguration(oldConfiguration, "Failed to delete node " + node, ex);
            removeDone.completeExceptionally(ex);
        }

        return removeDone;
    }

    private void transferLeader(AdminReplicationGroup context, String node)
            throws InterruptedException, java.util.concurrent.ExecutionException {
        String leader = raftGroupServiceFactory.getLeader(context.getName());
        if (context.getMembers().size() > 1 && node.equals(leader)) {
            logger.info("{}: leader is {}", context, leader);
            raftGroupServiceFactory.getRaftGroupService(context.getName()).transferLeadership(context.getName())
                                   .get();
            leader = raftGroupServiceFactory.getLeader(context.getName());
            int retries = 25;
            while ((leader == null || leader.equals(node)) && retries > 0) {
                Thread.sleep(250);
                leader = raftGroupServiceFactory.getLeader(context.getName());
                retries--;
            }
            if (leader == null || leader.equals(node)) {
                throw new MessagingPlatformException(ErrorCode.OTHER, "Moving leader to other node failed");
            }

            logger.info("{}: leader changed to {}", context, leader);
        }
    }

    /**
     * Deletes a node from the AxonServer cluster.
     * <p>
     * Process:
     * <ol>
     * <li>First remove the node from all non-admin contexts (unless the node is the only member of the context)</li>
     * <li>Then, remove the node from the admin context, if it is member</li>
     * <li>Append deleteNode entry to the admin log</li>
     * </ol>
     * </p>
     *
     * @param nodeName name of the node to delete
     */
    @Override
    public void deleteNode(String nodeName) {
        ClusterNode clusterNode = clusterController.getNode(nodeName);
        if (clusterNode == null) {
            logger.info("Delete Node: {} - Node not found.", nodeName);
            return;
        }
        clusterNode.getReplicationGroups()
                   .forEach(contextClusterNode -> taskPublisher.publishScheduledTask(getAdmin(),
                                                                                     PrepareDeleteNodeFromContextTask.class
                                                                                             .getName(),
                                                                                     new NodeContext(nodeName,
                                                                                                     contextClusterNode
                                                                                                             .getReplicationGroup()
                                                                                                             .getName(),
                                                                                                     false),
                                                                                     Duration.ZERO));


        taskPublisher.publishScheduledTask(getAdmin(),
                                           UnregisterNodeTask.class.getName(),
                                           nodeName,
                                           Duration.ofSeconds(1));
    }

    @Override
    public void deleteNodeIfEmpty(String name) {
        ClusterNode clusterNode = clusterController.getNode(name);
        if (clusterNode == null) {
            return;
        }

        if (clusterNode.getReplicationGroups()
                       .stream()
                       .anyMatch(c -> c.getReplicationGroup().getMembers().size() > 1)) {
            throw new TransientException("Node still member of contexts.");
        }

        appendToAdmin(DeleteNode.class.getName(),
                      DeleteNode.newBuilder().setNodeName(name).build().toByteArray());
    }

    @Override
    public CompletableFuture<Void> addContext(String replicationGroupName, String contextName,
                                              Map<String, String> metaData) {
        if (!Feature.MULTI_CONTEXT.enabled(limits)) {
            throw new MessagingPlatformException(ErrorCode.CONTEXT_CREATION_NOT_ALLOWED,
                                                 "License does not allow creating contexts");
        }

        if (isAdmin(replicationGroupName) && !isAdmin(contextName)) {
            throw new MessagingPlatformException(ErrorCode.INVALID_CONTEXT_NAME,
                                                 "Cannot add contexts to the admin replication group");
        }
        return doAddContext(replicationGroupName, contextName, new HashMap<>(metaData));
    }

    private CompletableFuture<Void> doAddContext(String replicationGroupName, String contextName,
                                                 Map<String, String> metaData) {
        metaData.putIfAbsent(ContextPropertyDefinition.EVENT_INDEX_FORMAT.key(),
                             EmbeddedDBPropertiesProvider.JUMP_SKIP_INDEX);
        metaData.putIfAbsent(ContextPropertyDefinition.SNAPSHOT_INDEX_FORMAT.key(),
                             EmbeddedDBPropertiesProvider.JUMP_SKIP_INDEX);
        ReplicationGroupContext contextDefinition = ReplicationGroupContext.newBuilder()
                                                                           .setReplicationGroupName(replicationGroupName)
                                                                           .setContextName(contextName)
                                                                           .putAllMetaData(metaData)
                                                                           .build();
        return raftGroupServiceFactory.getRaftGroupService(replicationGroupName)
                                      .appendEntry(replicationGroupName, contextDefinition)
                                      .thenAccept(v -> {
                                          addWildcardApps(replicationGroupName,
                                                          contextName);
                                          addWildcardUsers(replicationGroupName,
                                                           contextName);
                                          appendToAdmin(AdminContextConsumer.ENTRY_TYPE,
                                                        contextDefinition.toByteArray());
                                      }).exceptionally(ex -> {
                    logger.warn("{}: failed to complete add context",
                                replicationGroupName,
                                ex);
                    return null;
                });
    }

    @Override
    public void deleteContext(String name, boolean preserveEventStore) {
        if (isAdmin(name)) {
            throw new MessagingPlatformException(ErrorCode.CANNOT_DELETE_INTERNAL_CONTEXT,
                                                 String.format("Deletion of internal context %s not allowed",
                                                               name));
        }
        AdminContext context = contextController.getContext(name);
        if (context == null) {
            return;
        }
        applicationEventPublisher.publishEvent(new ContextEvents.ContextPreDelete(name));
        DeleteContextRequest deleteContextRequest = DeleteContextRequest.newBuilder()
                                                                        .setContext(name)
                                                                        .setReplicationGroupName(context.getReplicationGroup()
                                                                                                        .getName())
                                                                        .setPreserveEventstore(preserveEventStore)
                                                                        .build();
        raftGroupServiceFactory.getRaftGroupService(deleteContextRequest.getReplicationGroupName())
                               .appendEntry(deleteContextRequest.getReplicationGroupName(),
                                            deleteContextRequest)
                               .whenComplete((r, ex) ->
                                             {
                                                 if (ex == null) {
                                                     appendToAdmin(AdminDeleteContextConsumer.ENTRY_TYPE,
                                                                   deleteContextRequest.toByteArray());
                                                 }
                                             }
                               );
    }


    private void addWildcardUsers(String replicationGroup, String context) {
        Set<io.axoniq.axonserver.access.jpa.User> users = userController.getUsers().stream()
                                                                        .map(io.axoniq.axonserver.access.jpa.User::newContextPermissions)
                                                                        .filter(user -> !user.getRoles().isEmpty())
                                                                        .collect(Collectors.toSet());

        if (!users.isEmpty()) {
            users.forEach(user -> {
                ContextUser contextUser = UserProtoConverter.createContextUser(context, user);
                getFuture(raftGroupServiceFactory.getRaftGroupService(replicationGroup)
                                                 .appendEntry(replicationGroup, contextUser));
            });
        }
    }

    private void addWildcardApps(String replicationGroup, String context) {
        Set<AdminApplication> apps = applicationController.getApplications().stream()
                                                          .map(AdminApplication::newContextPermissions)
                                                          .filter(app -> !app.getContexts().isEmpty())
                                                          .collect(Collectors.toSet());

        if (!apps.isEmpty()) {
            apps.forEach(app -> {
                ContextApplication contextApplication =
                        ContextApplication.newBuilder()
                                          .setContext(context)
                                          .setName(app.getName())
                                          .setHashedToken(app.getHashedToken())
                                          .setTokenPrefix(app.getTokenPrefix())
                                          .addAllRoles(app.getContexts().stream().findFirst()
                                                          .map(ac ->
                                                                       ac.getRoles().stream()
                                                                         .map(AdminApplicationContextRole::getRole)
                                                                         .collect(
                                                                                 Collectors.toList())
                                                          ).orElse(Collections.emptyList())).build();

                getFuture(raftGroupServiceFactory.getRaftGroupService(replicationGroup)
                                                 .appendEntry(replicationGroup, contextApplication));
            });
        }
    }

    @Override
    public UpdateLicense join(NodeInfo nodeInfo) {
        if (!Feature.CLUSTERING.enabled(limits)) {
            throw new MessagingPlatformException(ErrorCode.CLUSTER_NOT_ALLOWED,
                                                 "License does not allow clustering of Axon servers");
        }

        if (clusterController.nodes().count() + 1 > limits.getMaxClusterSize()) {
            throw new MessagingPlatformException(ErrorCode.MAX_CLUSTER_SIZE_REACHED,
                                                 "Maximum allowed number of nodes reached: " + limits
                                                         .getMaxClusterSize());
        }

        RaftNode adminNode = grpcRaftController.getRaftNode(getAdmin());
        if (!adminNode.isLeader()) {
            throw new MessagingPlatformException(ErrorCode.NODE_IS_REPLICA,
                                                 "Send join request to the leader of _admin context: " + adminNode
                                                         .getLeaderName());
        }

        byte[] licenseContent = readLicense();

        List<String> replicationGroupNames = replicationGroupsToJoin(nodeInfo);

        String nodeLabel = generateNodeLabel(nodeInfo.getNodeName());
        Node node = Node.newBuilder().setNodeId(nodeLabel)
                        .setHost(nodeInfo.getInternalHostName())
                        .setPort(nodeInfo.getGrpcInternalPort())
                        .setNodeName(nodeInfo.getNodeName())
                        .build();
        sendToAdmin(AdminNodeConsumer.class.getName(), nodeInfo.toByteArray());

        replicationGroupNames.forEach(replicationGroupName -> {
            ReplicationGroupConfiguration oldConfiguration = null;
            ReplicationGroupConfiguration newContext;
            try {
                AdminReplicationGroup replicationGroup = adminReplicationGroupController
                        .findByName(replicationGroupName).orElse(null);
                if (replicationGroup != null) {
                    if (replicationGroup.getMemberNames().contains(nodeInfo.getNodeName())) {
                        logger.info("{}: Node {} is already member", replicationGroup, node.getNodeName());
                    } else {
                        oldConfiguration = createReplicationGroupConfigurationBuilder(replicationGroup).build();
                        ReplicationGroupConfiguration old = oldConfiguration;
                        ReplicationGroupConfiguration pending = ReplicationGroupConfiguration.newBuilder(old)
                                                                                             .setPending(true)
                                                                                             .build();

                        appendToAdmin(ReplicationGroupConfiguration.class.getName(), pending.toByteArray());
                        raftGroupServiceFactory.getRaftGroupService(replicationGroupName)
                                               .addServer(replicationGroupName, node)
                                               .thenAccept(result -> handleReplicationGroupUpdateResult(
                                                       replicationGroupName,
                                                       result))
                                               .exceptionally(e -> resetAdminConfiguration(old,
                                                                                           "Failed to add " + node
                                                                                                   .getNodeName(),
                                                                                           e));
                    }
                } else {
                    replicationGroup = new AdminReplicationGroup();
                    replicationGroup.setName(replicationGroupName);
                    oldConfiguration = createReplicationGroupConfigurationBuilder(replicationGroup).build();
                    ReplicationGroupConfiguration old = oldConfiguration;
                    ReplicationGroupConfiguration pending = ReplicationGroupConfiguration.newBuilder(old).setPending(
                            true).build();
                    newContext = createReplicationGroupConfigurationBuilder(replicationGroup)
                            .addNodes(newNodeInfoWithLabel(nodeLabel, nodeInfo)).build();
                    appendToAdmin(ReplicationGroupConfiguration.class.getName(), pending.toByteArray());

                    raftGroupServiceFactory.getRaftGroupServiceForNode(ClusterNode.from(nodeInfo))
                                           .initReplicationGroup(replicationGroupName, Collections.singletonList(node))
                                           .thenApply(result -> doAddContext(replicationGroupName,
                                                                             replicationGroupName,
                                                                             new HashMap<>()))
                                           .thenAccept(addContextResult -> {
                                               ReplicationGroupConfiguration confirmConfiguration = ReplicationGroupConfiguration
                                                       .newBuilder(newContext).setPending(false).build();
                                               try {
                                                   sendToAdmin(confirmConfiguration.getClass().getName(),
                                                               confirmConfiguration.toByteArray());
                                               } catch (Exception second) {
                                                   logger.warn(
                                                           "{}: Error while restoring updated configuration in admin {}",
                                                           replicationGroupName,
                                                           second.getMessage(), second);
                                               }
                                           }).exceptionally(throwable -> resetAdminConfiguration(old,
                                                                                                 "Error while creating context",
                                                                                                 throwable));
                }
            } catch (Exception ex) {
                resetAdminConfiguration(oldConfiguration, "Error while adding node " + node.getNodeName(), ex);
            }
        });

        return UpdateLicense.newBuilder().setLicense(ByteString.copyFrom(licenseContent)).build();
    }

    private byte[] readLicense() {
        return licenseManager.readLicense();
    }

    @NotNull
    private List<String> replicationGroupsToJoin(NodeInfo nodeInfo) {
        List<String> replicationGroupNames = nodeInfo.getContextsList().stream().map(ContextRole::getName).collect(
                Collectors
                        .toList());
        if ((replicationGroupNames.size() == 1) && replicationGroupNames.get(0).equals(CONTEXT_NONE)) {
            logger.debug("join(): Joining to no contexts.");
            replicationGroupNames.clear();
        } else if (replicationGroupNames.isEmpty()) {
            logger.debug("join(): Joining to all contexts.");
            replicationGroupNames = adminReplicationGroupController.findAll().stream()
                                                                   .map(AdminReplicationGroup::getName).collect(
                            Collectors.toList());
        } else {
            logger.debug("join(): Joining to a specified set of contexts.");
        }
        return replicationGroupNames;
    }

    private ReplicationGroupConfiguration createReplicationGroupConfiguration(String name,
                                                                              ReplicationGroupUpdateConfirmation result) {
        ReplicationGroupConfiguration.Builder builder = ReplicationGroupConfiguration.newBuilder()
                                                                                     .setReplicationGroupName(name)
                                                                                     .setPending(result.getPending());

        result.getMembersList().forEach(contextMember -> {
            ClusterNode node = clusterController.getNode(contextMember.getNodeName());
            if (node == null) {
                logger.warn("Could not find {} in admin", contextMember.getNodeName());
                node = new ClusterNode(contextMember.getNodeName(),
                                       "",
                                       contextMember.getHost(),
                                       -1,
                                       contextMember.getPort(),
                                       -1);
            }

            builder.addNodes(NodeInfoWithLabel.newBuilder().setNode(node.toNodeInfo())
                                              .setLabel(contextMember.getNodeId())
                                              .setRole(contextMember.getRole())
            );
        });

        return builder.build();
    }

    private void appendToAdmin(String name, byte[] bytes) {
        getFuture(sendToAdmin(name, bytes), 5, TimeUnit.SECONDS);
    }

    private CompletableFuture<Void> sendToAdmin(String name, byte[] bytes) {
        return raftGroupServiceFactory.getRaftGroupService(getAdmin()).appendEntry(getAdmin(), name, bytes);
    }

    private NodeInfoWithLabel newNodeInfoWithLabel(String nodeLabel, NodeInfo nodeInfo) {
        return NodeInfoWithLabel.newBuilder()
                                .setLabel(nodeLabel)
                                .setNode(nodeInfo)
                                .setRole(Role.PRIMARY)
                                .build();
    }

    @Override
    public void init(List<String> contexts) {
        if (!canInit()) {
            throw new MessagingPlatformException(ErrorCode.ALREADY_MEMBER_OF_CLUSTER,
                                                 "Node is already member of cluster or initialized before");
        }
        for (String context : contexts) {
            if (!contextNameValidation.test(context)) {
                throw new MessagingPlatformException(ErrorCode.INVALID_CONTEXT_NAME,
                                                     "Invalid context name: " + context);
            }
        }
        logger.info("Initialization of this node with following contexts: {}", contexts);
        CompetableFutureUtils.getFuture(init(getAdmin(), getAdmin()), 15, TimeUnit.MINUTES);
        contexts.forEach(ctx -> CompetableFutureUtils.getFuture(init(ctx, ctx), 15, TimeUnit.MINUTES));
    }

    private boolean canInit() {
        return adminReplicationGroupController.findAll().isEmpty() && clusterController.getRemoteConnections()
                                                                                       .isEmpty();
    }

    private CompletableFuture<Void> init(String replicationGroupName, String contextName) {
        String nodeName = messagingPlatformConfiguration.getName();
        String nodeLabelForContext = generateNodeLabel(nodeName);
        RaftGroup raftGroup = grpcRaftController.initRaftGroup(replicationGroupName,
                                                               nodeLabelForContext,
                                                               messagingPlatformConfiguration.getName());
        NodeInfo nodeInfo = NodeInfo
                .newBuilder()
                .setNodeName(nodeName)
                .setGrpcInternalPort(messagingPlatformConfiguration.getInternalPort())
                .setInternalHostName(messagingPlatformConfiguration.getFullyQualifiedInternalHostname())
                .setGrpcPort(messagingPlatformConfiguration.getPort())
                .setHttpPort(messagingPlatformConfiguration.getHttpPort())
                .setHostName(messagingPlatformConfiguration.getFullyQualifiedHostname())
                .build();
        ReplicationGroupConfiguration contextConfiguration = ReplicationGroupConfiguration
                .newBuilder()
                .setReplicationGroupName(replicationGroupName)
                .addNodes(NodeInfoWithLabel.newBuilder()
                                           .setNode(nodeInfo)
                                           .setRole(Role.PRIMARY)
                                           .setLabel(nodeLabelForContext))
                .build();
        RaftNode adminLeader = grpcRaftController.waitForLeader(grpcRaftController.getRaftGroup(getAdmin()));
        getFuture(adminLeader
                          .appendEntry(ReplicationGroupConfiguration.class.getName(),
                                       contextConfiguration.toByteArray()));
        grpcRaftController.waitForLeader(raftGroup);
        return doAddContext(replicationGroupName, contextName, new HashMap<>());
    }

    @Override
    public Application refreshToken(Application application) {
        try {
            AdminApplication adminApplication = applicationController.get(application.getName());

            String token = isEmpty(application.getToken()) ? UUID.randomUUID().toString() : application.getToken();
            Application updatedApplication = Application.newBuilder(ApplicationProtoConverter
                                                                            .createApplication(adminApplication))
                                                        .setToken(applicationController.hash(token))
                                                        .setTokenPrefix(AdminApplicationController.tokenPrefix(token))
                                                        .build();
            return distributeApplication(updatedApplication, token);
        } catch (ApplicationNotFoundException notFound) {
            throw new MessagingPlatformException(ErrorCode.NO_SUCH_APPLICATION,
                                                 "Application not found");
        }
    }

    @Override
    public Application updateApplication(Application application) {
        validateContextNames(application);
        AdminApplication storedApplication = null;
        try {
            storedApplication = applicationController.get(application.getName());
        } catch (ApplicationNotFoundException ane) {
            logger.debug("JpaApplication not found {}, creating new", application.getName());
        }
        String token = "Token already returned";
        String hashedToken;
        String tokenPrefix;
        if (storedApplication == null) {
            if (StringUtils.isEmpty(application.getToken())) {
                token = UUID.randomUUID().toString();
            } else {
                token = application.getToken();
            }
            hashedToken = applicationController.hash(token);
            tokenPrefix = AdminApplicationController.tokenPrefix(token);
        } else {
            hashedToken = storedApplication.getHashedToken();
            tokenPrefix = storedApplication.getTokenPrefix() == null ? "" : storedApplication.getTokenPrefix();
        }
        Application updatedApplication = Application.newBuilder(application)
                                                    .setToken(hashedToken)
                                                    .setTokenPrefix(tokenPrefix)
                                                    .build();
        return distributeApplication(updatedApplication, token);
    }

    private Application distributeApplication(Application updatedApplication,
                                              String token) {
        appendToAdmin(Application.class.getName(), updatedApplication.toByteArray());
        contextController.getContexts().forEach(c -> updateApplicationInGroup(updatedApplication, c));
        return Application.newBuilder(updatedApplication).setToken(token).build();
    }

    private void updateApplicationInGroup(Application updatedApplication, AdminContext c) {
        try {
            Collection<String> roles = getRolesPerContext(updatedApplication,
                                                          c.getName());
            ContextApplication contextApplication =
                    ContextApplication.newBuilder()
                                      .setContext(c.getName())
                                      .setName(updatedApplication.getName())
                                      .setHashedToken(updatedApplication.getToken())
                                      .setTokenPrefix(updatedApplication
                                                              .getTokenPrefix())
                                      .addAllRoles(roles).build();

            raftGroupServiceFactory.getRaftGroupService(c.getReplicationGroup().getName())
                                   .appendEntry(c.getReplicationGroup().getName(), contextApplication).get();
        } catch (Exception ex) {
            logger.warn("Failed to update application in context {}", c.getName(), ex);
        }
    }

    @Override
    @Transactional
    public void updateUser(User request) {
        validateContextNames(request);

        appendToAdmin(User.class.getName(), request.toByteArray());

        String password = userController.getPassword(request.getName());
        contextController.getContexts().forEach(c -> updateUserInContext(request, password, c));
    }

    private void validateContextNames(User request) {
        Set<String> validContexts = contextController.getContexts().map(AdminContext::getName).collect(Collectors
                                                                                                               .toSet());
        Set<String> invalidContexts = request.getRolesList()
                                             .stream()
                                             .map(UserContextRole::getContext)
                                             .filter(role -> !role.equals("*"))
                                             .filter(role -> !validContexts.contains(role))
                                             .collect(Collectors.toSet());

        if (!invalidContexts.isEmpty()) {
            throw new MessagingPlatformException(ErrorCode.CONTEXT_NOT_FOUND,
                                                 String.format("Context unknown: %s", invalidContexts));
        }
    }

    private void validateContextNames(Application application) {
        Set<String> validContexts = contextController.getContexts().map(AdminContext::getName).collect(Collectors
                                                                                                               .toSet());
        Set<String> invalidContexts = application.getRolesPerContextList()
                                                 .stream()
                                                 .map(ApplicationContextRole::getContext)
                                                 .filter(role -> !role.equals("*"))
                                                 .filter(role -> !validContexts.contains(role))
                                                 .collect(Collectors.toSet());

        if (!invalidContexts.isEmpty()) {
            throw new MessagingPlatformException(ErrorCode.CONTEXT_NOT_FOUND,
                                                 String.format("Context unknown: %s", invalidContexts));
        }
    }

    private void updateUserInContext(User request, String password, AdminContext context) {
        Set<UserContextRole> roles = getUserRolesPerContext(request.getRolesList(), context.getName());
        ContextUser contextUser =
                ContextUser.newBuilder()
                           .setContext(context.getName())
                           .setUser(User.newBuilder(request)
                                        .setPassword(getOrDefault(password, ""))
                                        .clearRoles()
                                        .addAllRoles(roles).build())
                           .build();
        getFuture(raftGroupServiceFactory.getRaftGroupService(context.getReplicationGroup().getName())
                                         .appendEntry(context.getReplicationGroup().getName(), contextUser));
    }

    private Set<UserContextRole> getUserRolesPerContext(List<UserContextRole> rolesList, String context) {
        return rolesList.stream()
                        .filter(r -> r.getContext().equals(context) || r.getContext().equals("*"))
                        .collect(Collectors.toSet());
    }

    private Collection<String> getRolesPerContext(Application application, String name) {
        Set<String> roles = new HashSet<>();
        for (ApplicationContextRole applicationContextRole : application.getRolesPerContextList()) {
            if (name.equals(applicationContextRole.getContext())) {
                roles.addAll(applicationContextRole.getRolesList());
            }

            if (applicationContextRole.getContext().equals("*")) {
                roles.addAll(applicationContextRole.getRolesList());
            }
        }
        return roles;
    }

    @Override
    public void updateProcessorLoadBalancing(ProcessorLBStrategy processorLBStrategy) {
        AdminContext context = contextController.getContext(processorLBStrategy.getContext());
        appendToAdmin(ProcessorLBStrategy.class.getName(), processorLBStrategy.toByteArray());
        raftGroupServiceFactory.getRaftGroupService(context.getReplicationGroup().getName())
                               .appendEntry(context.getReplicationGroup().getName(),
                                            ReplicationGroupProcessorLoadBalancing.class.getName(),
                                            processorLBStrategy.toByteArray());
    }

    private ReplicationGroupConfiguration.Builder createReplicationGroupConfigurationBuilder(
            AdminReplicationGroup replicationGroup) {
        ReplicationGroupConfiguration.Builder groupConfigurationBuilder = ReplicationGroupConfiguration.newBuilder()
                                                                                                       .setReplicationGroupName(
                                                                                                               replicationGroup
                                                                                                                       .getName());
        replicationGroup.getMembers().forEach(n -> groupConfigurationBuilder
                .setPending(replicationGroup.isChangePending())
                .addNodes(NodeInfoWithLabel.newBuilder()
                                           .setNode(n.getClusterNode().toNodeInfo())
                                           .setLabel(n.getClusterNodeLabel())
                                           .setRole(n.getRole())
                                           .build()));
        return groupConfigurationBuilder;
    }

    @Override
    @Transactional
    public void deleteUser(User user) {
        io.axoniq.axonserver.access.jpa.User userInDb = userController.findUser(user.getName());
        if (userInDb == null) {
            return;
        }
        Set<String> contexts = userInDb.getRoles().stream().map(UserRole::getContext).collect(Collectors.toSet());

        appendToAdmin(DELETE_USER, user.toByteArray());
        if (contexts.contains("*")) {
            contextController.getContexts()
                             .forEach(context -> appendContextUserForDelete(user, context));
        } else {
            contexts
                    .forEach(contextName -> {
                        AdminContext context = contextController.getContext(contextName);
                        appendContextUserForDelete(user, context);
                    });
        }
    }

    private void appendContextUserForDelete(User user, AdminContext context) {
        raftGroupServiceFactory
                .getRaftGroupService(context.getReplicationGroup().getName())
                .appendEntry(context.getReplicationGroup().getName(),
                             ContextUser.newBuilder()
                                        .setContext(context.getName())
                                        .setUser(User.newBuilder()
                                                     .setName(user.getName()))
                                        .build());
    }

    @Override
    @Transactional
    public void deleteApplication(Application application) {
        AdminApplication applicationInDb = applicationController.get(application.getName());
        if (applicationInDb == null) {
            return;
        }
        Set<String> contexts = applicationInDb.getContexts().stream().map(AdminApplicationContext::getContext).collect(
                Collectors.toSet());

        appendToAdmin(DELETE_APPLICATION, application.toByteArray());
        if (contexts.contains("*")) {
            contextController.getContexts()
                             .forEach(context -> appendContextApplicationForDelete(application
                                     , context));
        } else {
            contexts.forEach(contextName -> {
                AdminContext context = contextController.getContext(contextName);
                appendContextApplicationForDelete(application, context);
            });
        }
    }

    private void appendContextApplicationForDelete(Application application, AdminContext context) {
        raftGroupServiceFactory
                .getRaftGroupService(context.getReplicationGroup().getName())
                .appendEntry(context.getReplicationGroup().getName(),
                             ContextApplication.newBuilder()
                                               .setContext(context.getName())
                                               .setName(application.getName())
                                               .build());
    }

    /**
     * Event handler for {@link io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents.LeaderConfirmation} events.
     * Checks if there were configuration changes pending when the leader changed, and if so, removes the flag.
     *
     * @param leaderConfirmation the event
     */
    @EventListener
    public void on(ClusterEvents.LeaderConfirmation leaderConfirmation) {
        try {
            checkPendingChanges(leaderConfirmation.replicationGroup());
        } catch (IllegalStateException ignore) {
            // Node may be starting or stopping
        }
    }

    /**
     * Event handler for {@link io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents.BecomeLeader} events.
     * Checks if there were configuration changes pending when the leader changed, and if so, removes the flag.
     *
     * @param becomeLeader the event
     */
    @EventListener
    @Order(5)
    public void on(ClusterEvents.BecomeLeader becomeLeader) {
        checkPendingChanges(becomeLeader.replicationGroup());
    }

    private void checkPendingChanges(String contextName) {
        try {
            RaftNode admin = grpcRaftController.getRaftNode(getAdmin());
            if (admin.isLeader()) {
                adminReplicationGroupController.findByName(contextName).ifPresent(replicationGroup -> {
                    if (replicationGroup.isChangePending()) {
                        ReplicationGroupConfiguration configuration = createReplicationGroupConfigurationBuilder(
                                replicationGroup).setPending(false)
                                                 .build();
                        admin.appendEntry(ReplicationGroupConfiguration.class.getName(), configuration.toByteArray());
                    }
                });
            }
        } catch (MessagingPlatformException ex) {
            if (!ErrorCode.CONTEXT_NOT_FOUND.equals(ex.getErrorCode())) {
                logger.warn("Error checking pending changes", ex);
            }
        }
    }
}
