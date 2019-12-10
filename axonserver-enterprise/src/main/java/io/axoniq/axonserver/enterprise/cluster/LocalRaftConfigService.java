package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.access.application.ApplicationController;
import io.axoniq.axonserver.access.application.ApplicationNotFoundException;
import io.axoniq.axonserver.access.application.JpaApplication;
import io.axoniq.axonserver.access.user.UserController;
import io.axoniq.axonserver.cluster.RaftNode;
import io.axoniq.axonserver.cluster.util.RoleUtils;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import io.axoniq.axonserver.enterprise.context.ContextController;
import io.axoniq.axonserver.enterprise.context.ContextNameValidation;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.enterprise.jpa.Context;
import io.axoniq.axonserver.enterprise.jpa.ContextClusterNode;
import io.axoniq.axonserver.enterprise.logconsumer.AdminNodeConsumer;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.ApplicationProtoConverter;
import io.axoniq.axonserver.grpc.UserProtoConverter;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.cluster.Role;
import io.axoniq.axonserver.grpc.internal.Application;
import io.axoniq.axonserver.grpc.internal.ApplicationContextRole;
import io.axoniq.axonserver.grpc.internal.ContextApplication;
import io.axoniq.axonserver.grpc.internal.ContextConfiguration;
import io.axoniq.axonserver.grpc.internal.ContextRole;
import io.axoniq.axonserver.grpc.internal.ContextUpdateConfirmation;
import io.axoniq.axonserver.grpc.internal.ContextUser;
import io.axoniq.axonserver.grpc.internal.DeleteNode;
import io.axoniq.axonserver.grpc.internal.LoadBalanceStrategy;
import io.axoniq.axonserver.grpc.internal.NodeInfo;
import io.axoniq.axonserver.grpc.internal.NodeInfoWithLabel;
import io.axoniq.axonserver.grpc.internal.ProcessorLBStrategy;
import io.axoniq.axonserver.grpc.internal.User;
import io.axoniq.axonserver.grpc.internal.UserContextRole;
import io.axoniq.axonserver.util.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static io.axoniq.axonserver.RaftAdminGroup.getAdmin;
import static io.axoniq.axonserver.RaftAdminGroup.isAdmin;
import static io.axoniq.axonserver.enterprise.CompetableFutureUtils.getFuture;
import static io.axoniq.axonserver.enterprise.logconsumer.DeleteApplicationConsumer.DELETE_APPLICATION;
import static io.axoniq.axonserver.enterprise.logconsumer.DeleteLoadBalancingStrategyConsumer.DELETE_LOAD_BALANCING_STRATEGY;
import static io.axoniq.axonserver.enterprise.logconsumer.DeleteUserConsumer.DELETE_USER;
import static io.axoniq.axonserver.rest.ClusterRestController.CONTEXT_NONE;
import static io.axoniq.axonserver.util.StringUtils.getOrDefault;

/**
 * Service to orchestrate configuration changes. This service is executed on the leader of the _admin context.
 *
 * @author Marc Gathier
 */
@Component
class LocalRaftConfigService implements RaftConfigService {

    private static final long MAX_PENDING_TIME = TimeUnit.SECONDS.toMillis(30);
    private final GrpcRaftController grpcRaftController;
    private final ContextController contextController;
    private final ClusterController clusterController;
    private final RaftGroupServiceFactory raftGroupServiceFactory;
    private final ApplicationController applicationController;
    private final UserController userController;
    private final MessagingPlatformConfiguration messagingPlatformConfiguration;
    private final Predicate<String> contextNameValidation = new ContextNameValidation();
    private final CopyOnWriteArraySet<String> contextsInProgress = new CopyOnWriteArraySet<>();
    private final Logger logger = LoggerFactory.getLogger(LocalRaftConfigService.class);


    public LocalRaftConfigService(GrpcRaftController grpcRaftController,
                                  ContextController contextController,
                                  ClusterController clusterController,
                                  RaftGroupServiceFactory raftGroupServiceFactory,
                                  ApplicationController applicationController,
                                  UserController userController,
                                  MessagingPlatformConfiguration messagingPlatformConfiguration) {
        this.grpcRaftController = grpcRaftController;
        this.contextController = contextController;
        this.clusterController = clusterController;
        this.raftGroupServiceFactory = raftGroupServiceFactory;
        this.applicationController = applicationController;
        this.userController = userController;
        this.messagingPlatformConfiguration = messagingPlatformConfiguration;
    }

    @Override
    public void addNodeToContext(String context, String node, Role role) {
        logger.info("Add node request invoked for node: {} - and context: {}", node, context);
        Context contextDefinition = contextController.getContext(context);

        if (contextDefinition == null) {
            throw new MessagingPlatformException(ErrorCode.CONTEXT_NOT_FOUND,
                                                 String.format("Context %s not found", context));
        }

        if (contextDefinition.isChangePending()
                && contextDefinition.getPendingSince().getTime() > System.currentTimeMillis() - MAX_PENDING_TIME) {
            throw new MessagingPlatformException(ErrorCode.CONTEXT_UPDATE_IN_PROGRESS, context + ": pending update");
        }
        ClusterNode clusterNode = clusterController.getNode(node);
        if (clusterNode == null) {
            throw new MessagingPlatformException(ErrorCode.NO_SUCH_NODE, String.format("Node %s not found", node));
        }
        if (clusterNode.getContextNames().contains(context)) {
            return;
        }
        ContextConfiguration oldConfiguration = createContextConfigBuilder(contextDefinition).build();
        String nodeLabel = generateNodeLabel(node);
        Node raftNode = createNode(clusterNode, nodeLabel, role);

        ContextConfiguration contextConfiguration =
                ContextConfiguration.newBuilder(oldConfiguration).setPending(true)
                                    .build();

        appendToAdmin(ContextConfiguration.class.getName(), contextConfiguration.toByteArray());

        try {
            raftGroupServiceFactory.getRaftGroupService(context)
                                   .addNodeToContext(context, raftNode)
                                   .thenAccept(result -> handleContextUpdateResult(context, result))
                                   .exceptionally(e -> resetAdminConfiguration(oldConfiguration,
                                                                               "Failed to add node: " + node,
                                                                               e));
        } catch (RuntimeException throwable) {
            resetAdminConfiguration(oldConfiguration, "Failed to add node: " + node, throwable);
            throw throwable;
        }
    }

    private void handleContextUpdateResult(String context,
                                           ContextUpdateConfirmation result) {
        if (!result.getSuccess()) {
            logger.error("{}: {}", context, result.getMessage());
        }
        ContextConfiguration updatedConfiguration = createContextConfiguration(context, result);
        try {
            sendToAdmin(updatedConfiguration.getClass().getName(), updatedConfiguration.toByteArray());
        } catch (Exception exception) {
            logger.warn("{}: Error sending updated configuration to admin {}", context, exception.getMessage());
        }
    }

    private Void resetAdminConfiguration(ContextConfiguration oldConfiguration, String message, Throwable reason) {
        if (oldConfiguration == null) {
            logger.error("{}", message, reason);
            return null;
        }
        logger.error("{}: {}", oldConfiguration.getContext(), message, reason);
        try {
            appendToAdmin(oldConfiguration.getClass().getName(), oldConfiguration.toByteArray());
        } catch (Exception adminException) {
            logger.debug("{}: Error while restoring old configuration in admin {}",
                         oldConfiguration.getContext(),
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
    public void deleteContext(String context) {
        logger.info("Delete context invoked for context: {}", context);
        if (isAdmin(context)) {
            throw new MessagingPlatformException(ErrorCode.CANNOT_DELETE_INTERNAL_CONTEXT,
                                                 String.format("Deletion of internal context %s not allowed", context));
        }

        Context contextInAdmin = contextController.getContext(context);
        if (contextInAdmin == null) {
            logger.warn("Could not find context {} in admin tables, sending deleteContext to all nodes", context);
            clusterController.remoteNodeNames().forEach(node -> raftGroupServiceFactory.getRaftGroupServiceForNode(node)
                                                                                       .deleteContext(context, false));
            raftGroupServiceFactory.getRaftGroupServiceForNode(this.messagingPlatformConfiguration.getName())
                                   .deleteContext(context, false);
            contextsInProgress.remove(context);
            return;
        }
        Collection<String> nodeNames = contextInAdmin.getNodeNames();
        @SuppressWarnings("unchecked")
        CompletableFuture<Void>[] workers = new CompletableFuture[nodeNames.size()];

        ContextConfiguration contextConfiguration = createContextConfigBuilder(contextInAdmin)
                .setPending(true)
                .build();

        appendToAdmin(ContextConfiguration.class.getName(), contextConfiguration.toByteArray());

        int nodeIdx = 0;
        Iterable<String> nodes = new HashSet<>(nodeNames);
        for (String name : nodes) {
            workers[nodeIdx] = raftGroupServiceFactory.getRaftGroupServiceForNode(name).deleteContext(context, false);
            workers[nodeIdx].thenAccept(r -> nodeNames.remove(name));
            nodeIdx++;
        }

        CompletableFuture.allOf(workers).whenComplete((result, exception) -> {
            ContextConfiguration.Builder updatedContextConfigurationBuilder =
                    ContextConfiguration.newBuilder()
                                        .setContext(context);
            if (exception != null) {
                logger.warn("{}: Could not delete context from {}", context, String.join(",", nodeNames), exception);

                contextInAdmin.getNodes().stream().filter(c -> nodeNames.contains(c.getClusterNode().getName()))
                              .forEach(c -> updatedContextConfigurationBuilder.addNodes(NodeInfoWithLabel.newBuilder()
                                                                                                         .setLabel(c.getClusterNodeLabel())
                                                                                                         .setNode(c.getClusterNode()
                                                                                                                   .toNodeInfo())));
            }
            try {
                appendToAdmin(ContextConfiguration.class.getName(),
                              updatedContextConfigurationBuilder.build().toByteArray());
            } catch (Exception second) {
                logger.debug("{}: Error while updating configuration {}", context, second.getMessage());
            }
            contextsInProgress.remove(context);
        });
    }


    @Override
    public void deleteNodeFromContext(String context, String node) {
        logger.info("Delete node from context invoked for context: {} - and node: {}", context, node);
        Context contextDef = contextController.getContext(context);
        String nodeLabel = contextDef.getNodeLabel(node);

        if (!contextDef.getNodeNames().contains(node)) {
            throw new MessagingPlatformException(ErrorCode.NO_SUCH_NODE,
                                                 String.format("Node %s not found in context %s", node, context));
        }

        if (contextDef.getNodeNames().size() == 1) {
            throw new MessagingPlatformException(ErrorCode.CANNOT_REMOVE_LAST_NODE,
                                                 String.format(
                                                         "Node %s is last node in context %s, to delete the context use unregister context",
                                                               node,
                                                               context));
        }

        removeNodeFromContext(contextDef, node, nodeLabel);
    }

    private CompletableFuture<Void> removeNodeFromContext(Context context,
                                                          String node, String nodeLabel) {
        CompletableFuture<Void> removeDone = new CompletableFuture<>();
        ContextConfiguration oldConfiguration = createContextConfigBuilder(context)
                .build();
        try {
            ContextConfiguration contextConfiguration = ContextConfiguration.newBuilder(oldConfiguration)
                                                                            .setPending(true)
                                                                            .build();
            appendToAdmin(ContextConfiguration.class.getName(), contextConfiguration.toByteArray());

            transferLeader(context, node);

            raftGroupServiceFactory.getRaftGroupService(context.getName()).deleteNode(context.getName(), nodeLabel)
                                   .thenAccept(result -> handleContextUpdateResult(context.getName(), result))
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

    private void transferLeader(Context context, String node)
            throws InterruptedException, java.util.concurrent.ExecutionException {
        String leader = raftGroupServiceFactory.getLeader(context.getName());
        if (node.equals(leader)) {
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
     * @param name name of the node to delete
     */
    @Override
    public void deleteNode(String name) {
        ClusterNode clusterNode = clusterController.getNode(name);
        if (clusterNode == null) {
            logger.info("Delete Node: {} - Node not found.", name);
            return;
        }
        Set<ContextClusterNode> membersToDelete =
                clusterNode.getContexts()
                           .stream()
                           .filter(contextClusterNode -> contextClusterNode.getContext().getNodes().size() > 1)
                           .filter(contextClusterNode -> !isAdmin(contextClusterNode.getContext().getName()))
                           .collect(Collectors.toSet());


        List<CompletableFuture<Void>> completableFutures = new ArrayList<>();
        membersToDelete.forEach(contextClusterNode ->
                                        completableFutures.add(
                                                removeNodeFromContext(contextClusterNode.getContext(),
                                                                      name,
                                                                      contextClusterNode.getClusterNodeLabel())));

        getFuture(CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture[0]))
                                   .thenApply(r ->
                                                      clusterNode.getContext(getAdmin())
                                                                 .map(adminContext ->
                                                                              removeNodeFromContext(
                                                                                      adminContext.getContext(),
                                                                                      name,
                                                                                      adminContext
                                                                                              .getClusterNodeLabel()))
                                                                 .orElse(CompletableFuture.completedFuture(null)))
                                   .thenAccept(r -> {
                                       try {
                                           Thread.sleep((long) (grpcRaftController.electionTimeout() * 1.5));
                                           waitForAdminLeader();
                                           appendToAdmin(DeleteNode.class.getName(),
                                                         DeleteNode.newBuilder().setNodeName(name).build()
                                                                   .toByteArray());
                                       } catch (InterruptedException e) {
                                           Thread.currentThread().interrupt();
                                       }
                                   }));
    }

    private void waitForAdminLeader() throws InterruptedException {
        int retries = 25;
        while (retries > 0) {
            try {
                raftGroupServiceFactory.getRaftGroupService(getAdmin());
                return;
            } catch (RuntimeException re) {
                retries--;
                Thread.sleep(250);
            }
        }
    }

    @Override
    public void addContext(String context, List<String> nodes) {
        if (!contextsInProgress.add(context)) {
            throw new UnsupportedOperationException("The creation of the context is already in progress.");
        }
        Context contextDef = contextController.getContext(context);
        if (contextDef != null) {
            contextsInProgress.remove(context);
            throw new MessagingPlatformException(ErrorCode.CONTEXT_EXISTS,
                                                 String.format("Context %s already exists", context));
        }

        List<Node> raftNodes = new ArrayList<>();
        nodes.forEach(n -> {
            ClusterNode clusterNode = clusterController.getNode(n);
            String nodeLabel = generateNodeLabel(n);
            raftNodes.add(createNode(clusterNode, nodeLabel, Role.PRIMARY));
        });
        Node target = raftNodes.get(0);

        getFuture(
            raftGroupServiceFactory.getRaftGroupServiceForNode(target.getNodeName()).initContext(context, raftNodes)
                                   .thenAccept(contextConfiguration -> {
                                       ContextConfiguration completed = ContextConfiguration.newBuilder(
                                               contextConfiguration)
                                                                                            .setPending(false)
                                                                                            .build();
                                       appendToAdmin(ContextConfiguration.class.getName(),
                                                     completed.toByteArray());
                                   }).whenComplete((success, error) -> {
                contextsInProgress.remove(context);
                if (error != null) {
                    deleteContext(context);
                } else {
                    addWildcardApps(context);
                    addWildcardUsers(context);
                }
            }));
    }

    private void addWildcardUsers(String context) {
        userController.getUsers().stream()
                      .map(io.axoniq.axonserver.access.jpa.User::newContextPermissions)
                      .filter(user -> !user.getRoles().isEmpty())
                      .forEach(user -> {
                          ContextUser contextUser = UserProtoConverter.createContextUser(context, user);
                          getFuture(raftGroupServiceFactory.getRaftGroupService(context)
                                                           .updateUser(contextUser));
                      });
    }

    private void addWildcardApps(String context) {
        applicationController.getApplications().stream()
                             .map(JpaApplication::newContextPermissions)
                             .filter(app -> !app.getContexts().isEmpty())
                             .forEach(app -> {
                                 ContextApplication contextApplication =
                                         ContextApplication.newBuilder()
                                                           .setContext(context)
                                                           .setName(app.getName())
                                                           .setHashedToken(app.getHashedToken())
                                                           .setTokenPrefix(app.getTokenPrefix())
                                                           .addAllRoles(app.getContexts().stream().findFirst()
                                                                           .map(ac ->
                                                                                        ac.getRoles().stream()
                                                                                          .map(io.axoniq.axonserver.access.application.ApplicationContextRole::getRole)
                                                                                          .collect(
                                                                                                  Collectors.toList())
                                                                           ).orElse(Collections.emptyList())).build();

                                 getFuture(raftGroupServiceFactory.getRaftGroupService(context)
                                                                  .updateApplication(contextApplication));
                             });
    }

    @Override
    public void join(NodeInfo nodeInfo) {
        RaftNode adminNode = grpcRaftController.getRaftNode(getAdmin());
        if (!adminNode.isLeader()) {
            throw new MessagingPlatformException(ErrorCode.NODE_IS_REPLICA,
                                                 "Send join request to the leader of _admin context: " + adminNode
                                                         .getLeaderName());
        }
        List<String> contexts = contextsToJoin(nodeInfo);

        String nodeLabel = generateNodeLabel(nodeInfo.getNodeName());
        Node node = Node.newBuilder().setNodeId(nodeLabel)
                        .setHost(nodeInfo.getInternalHostName())
                        .setPort(nodeInfo.getGrpcInternalPort())
                        .setNodeName(nodeInfo.getNodeName())
                        .build();
        sendToAdmin(AdminNodeConsumer.class.getName(), nodeInfo.toByteArray());

        contexts.forEach(c -> {
            Context context = contextController.getContext(c);
            ContextConfiguration oldConfiguration = null;
            ContextConfiguration newContext;
            try {
                if (context != null) {
                    if (context.getNodeNames().contains(nodeInfo.getNodeName())) {
                        logger.info("{}: Node {} is already member", c, node.getNodeName());
                    } else {
                        oldConfiguration = createContextConfigBuilder(context).build();
                        ContextConfiguration old = oldConfiguration;
                        ContextConfiguration pending = ContextConfiguration.newBuilder(old).setPending(true)
                                                                           .build();

                        appendToAdmin(ContextConfiguration.class.getName(), pending.toByteArray());
                        raftGroupServiceFactory.getRaftGroupService(c)
                                               .addNodeToContext(c, node)
                                               .thenAccept(result -> handleContextUpdateResult(c, result))
                                               .exceptionally(e -> resetAdminConfiguration(old,
                                                                                           "Failed to add " + node
                                                                                                   .getNodeName(),
                                                                                           e));
                    }
                } else {
                    context = new Context(c);
                    oldConfiguration = createContextConfigBuilder(context).build();
                    ContextConfiguration old = oldConfiguration;
                    ContextConfiguration pending = ContextConfiguration.newBuilder(old).setPending(true).build();
                    newContext = createContextConfigBuilder(context)
                            .addNodes(newNodeInfoWithLabel(nodeLabel, nodeInfo)).build();
                    appendToAdmin(ContextConfiguration.class.getName(), pending.toByteArray());

                    raftGroupServiceFactory.getRaftGroupServiceForNode(ClusterNode.from(nodeInfo))
                                           .initContext(c, Collections.singletonList(node))
                                           .thenAccept(result -> {
                                               ContextConfiguration confirmConfiguration = ContextConfiguration
                                                       .newBuilder(newContext).setPending(false).build();
                                               try {
                                                   sendToAdmin(confirmConfiguration.getClass().getName(),
                                                               confirmConfiguration.toByteArray());
                                               } catch (Exception second) {
                                                   logger.warn(
                                                           "{}: Error while restoring updated configuration in admin {}",
                                                           c,
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
    }

    @NotNull
    private List<String> contextsToJoin(NodeInfo nodeInfo) {
        List<String> contexts = nodeInfo.getContextsList().stream().map(ContextRole::getName).collect(Collectors
                                                                                                              .toList());
        if ((contexts.size() == 1) && contexts.get(0).equals(CONTEXT_NONE)) {
            logger.debug("join(): Joining to no contexts.");
            contexts.clear();
        } else if (contexts.isEmpty()) {
            logger.debug("join(): Joining to all contexts.");
            contexts = contextController.getContexts().map(Context::getName).collect(Collectors.toList());
        } else {
            logger.debug("join(): Joining to a specified set of contexts.");
        }
        return contexts;
    }

    private ContextConfiguration createContextConfiguration(String context, ContextUpdateConfirmation result) {
        ContextConfiguration.Builder builder = ContextConfiguration.newBuilder()
                                                                   .setContext(context)
                                                                   .setPending(result.getPending());

        result.getMembersList().forEach(contextMember -> {
            ClusterNode node = clusterController.getNode(contextMember.getNodeName());
            if (node == null) {
                logger.warn("Could not find {} in admin", contextMember.getNodeName());
                node = new ClusterNode(contextMember.getNodeName(),
                                       null,
                                       contextMember.getHost(),
                                       null,
                                       contextMember.getPort(),
                                       null);
            }

            builder.addNodes(NodeInfoWithLabel.newBuilder().setNode(node.toNodeInfo())
                                              .setLabel(contextMember.getNodeId())
                                              .setRole(contextMember.getRole())
            );
        });

        return builder.build();
    }

    private void appendToAdmin(String name, byte[] bytes) {
        getFuture(raftGroupServiceFactory.getRaftGroupService(getAdmin())
                                         .appendEntry(getAdmin(), name, bytes), 5, TimeUnit.SECONDS);
    }

    private void sendToAdmin(String name, byte[] bytes) {
        raftGroupServiceFactory.getRaftGroupService(getAdmin()).appendEntry(getAdmin(), name, bytes);
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
        init(getAdmin());
        contexts.forEach(this::init);
    }

    private boolean canInit() {
        return !(contextController.getContexts().count() > 0 ||
                clusterController.remoteNodeNames().iterator().hasNext());
    }

    private void init(String contextName) {
        String nodeName = messagingPlatformConfiguration.getName();
        String nodeLabelForContext = generateNodeLabel(nodeName);
        grpcRaftController.initRaftGroup(contextName,
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
        ContextConfiguration contextConfiguration = ContextConfiguration
                .newBuilder()
                .setContext(contextName)
                .addNodes(NodeInfoWithLabel.newBuilder()
                                           .setNode(nodeInfo)
                                           .setRole(Role.PRIMARY)
                                           .setLabel(nodeLabelForContext))
                .build();
        RaftNode adminLeader = grpcRaftController.waitForLeader(grpcRaftController.getRaftGroup(getAdmin()));
        getFuture(adminLeader
                          .appendEntry(ContextConfiguration.class.getName(), contextConfiguration.toByteArray()));
    }

    @Override
    public Application refreshToken(Application application) {
        try {
            JpaApplication jpaApplication = applicationController.get(application.getName());

            String token = UUID.randomUUID().toString();
            Application updatedApplication = Application.newBuilder(ApplicationProtoConverter
                                                                            .createApplication(jpaApplication))
                                                        .setToken(applicationController.hash(token))
                                                        .setTokenPrefix(ApplicationController.tokenPrefix(token))
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
        JpaApplication storedApplication = null;
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
                hashedToken = applicationController.hash(token);
                tokenPrefix = ApplicationController.tokenPrefix(token);
            } else {
                token = application.getToken();
                hashedToken = applicationController.hash(token);
                tokenPrefix = ApplicationController.tokenPrefix(token);
            }
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

    private void updateApplicationInGroup(Application updatedApplication, Context c) {
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

            raftGroupServiceFactory.getRaftGroupService(c.getName())
                                   .updateApplication(contextApplication).get();
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
        contextController.getContexts().forEach(c -> updateUserInGroup(request, password, c.getName()));
    }

    private void validateContextNames(User request) {
        Set<String> validContexts = contextController.getContexts().map(Context::getName).collect(Collectors.toSet());
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
        Set<String> validContexts = contextController.getContexts().map(Context::getName).collect(Collectors.toSet());
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

    private void updateUserInGroup(User request, String password, String context) {
        Set<UserContextRole> roles = getUserRolesPerContext(request.getRolesList(), context);
        ContextUser contextUser =
                ContextUser.newBuilder()
                           .setContext(context)
                           .setUser(User.newBuilder(request)
                                        .setPassword(getOrDefault(password, ""))
                                        .clearRoles()
                                        .addAllRoles(roles).build())
                           .build();
        getFuture(raftGroupServiceFactory.getRaftGroupService(context)
                                         .updateUser(contextUser));
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
    public void updateLoadBalancingStrategy(LoadBalanceStrategy loadBalancingStrategy) {
        appendToAdmin(LoadBalanceStrategy.class.getName(), loadBalancingStrategy.toByteArray());
        contextController.getContexts()
                         .filter(c -> !isAdmin(c.getName()))
                         .forEach(c -> raftGroupServiceFactory.getRaftGroupService(c.getName())
                                                              .updateLoadBalancingStrategy(c.getName(),
                                                                                           loadBalancingStrategy));
    }


    @Override
    public void updateProcessorLoadBalancing(ProcessorLBStrategy processorLBStrategy) {
        appendToAdmin(ProcessorLBStrategy.class.getName(), processorLBStrategy.toByteArray());
        raftGroupServiceFactory.getRaftGroupService(processorLBStrategy.getContext())
                               .updateProcessorLoadBalancing(processorLBStrategy
                                                                     .getContext(),
                                                             processorLBStrategy);
    }

    private ContextConfiguration.Builder createContextConfigBuilder(Context context) {
        ContextConfiguration.Builder groupConfigurationBuilder = ContextConfiguration.newBuilder()
                                                                                     .setContext(context.getName());
        context.getNodes().forEach(n -> groupConfigurationBuilder
                .setPending(context.isChangePending())
                .addNodes(NodeInfoWithLabel.newBuilder()
                                           .setNode(n.getClusterNode().toNodeInfo())
                                           .setLabel(n.getClusterNodeLabel())
                                           .setRole(n.getRole())
                                           .build()));
        return groupConfigurationBuilder;
    }

    @Override
    public void deleteUser(User user) {
        appendToAdmin(DELETE_USER, user.toByteArray());
        contextController.getContexts().forEach(c -> updateUserInGroup(user, null, c.getName()));
    }

    @Override
    public void deleteApplication(Application application) {
        appendToAdmin(DELETE_APPLICATION, application.toByteArray());
        contextController.getContexts().forEach(c ->
                                                        raftGroupServiceFactory.getRaftGroupService(c.getName())
                                                                               .updateApplication(ContextApplication
                                                                                                          .newBuilder()
                                                                                                          .setContext(
                                                                                                                  c.getName())
                                                                                                          .setName(
                                                                                                                  application
                                                                                                                          .getName())
                                                                                                          .build()));
    }

    @Override
    public void deleteLoadBalancingStrategy(LoadBalanceStrategy loadBalancingStrategy) {
        appendToAdmin(DELETE_LOAD_BALANCING_STRATEGY, loadBalancingStrategy.toByteArray());
        contextController.getContexts()
                         .filter(c -> !isAdmin(c.getName()))
                         .forEach(c -> {
                             try {
                                 raftGroupServiceFactory.getRaftGroupService(c.getName())
                                                        .deleteLoadBalancingStrategy(c.getName(),
                                                                                     loadBalancingStrategy);
                             } catch (Exception ex) {
                                 logger.warn("{}: Failed to delete load balancing strategy {}",
                                             c,
                                             loadBalancingStrategy.getName());
                             }
                         });
    }

    /**
     * Event handler for {@link io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents.LeaderConfirmation} events.
     * Checks if there were configuration changes pending when the leader changed, and if so, removes the flag.
     *
     * @param leaderConfirmation the event
     */
    @EventListener
    public void on(ClusterEvents.LeaderConfirmation leaderConfirmation) {
        checkPendingChanges(leaderConfirmation.getContext());
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
        checkPendingChanges(becomeLeader.getContext());
    }

    private void checkPendingChanges(String contextName) {
        try {
            RaftNode admin = grpcRaftController.getRaftNode(getAdmin());
            if (admin.isLeader()) {
                Context context = contextController.getContext(contextName);
                if (context != null && context.isChangePending()) {
                    ContextConfiguration configuration = createContextConfigBuilder(context).setPending(false)
                                                                                            .build();
                    admin.appendEntry(ContextConfiguration.class.getName(), configuration.toByteArray());
                }
            }
        } catch (MessagingPlatformException ex) {
            if (!ErrorCode.CONTEXT_NOT_FOUND.equals(ex.getErrorCode())) {
                logger.warn("Error checking pending changes", ex);
            }
        }
    }
}