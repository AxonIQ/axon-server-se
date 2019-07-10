package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.access.application.ApplicationController;
import io.axoniq.axonserver.access.application.ApplicationNotFoundException;
import io.axoniq.axonserver.access.application.JpaApplication;
import io.axoniq.axonserver.cluster.RaftNode;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import io.axoniq.axonserver.enterprise.context.ContextController;
import io.axoniq.axonserver.enterprise.context.ContextNameValidation;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.enterprise.jpa.Context;
import io.axoniq.axonserver.enterprise.jpa.ContextClusterNode;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.ApplicationProtoConverter;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.internal.Application;
import io.axoniq.axonserver.grpc.internal.ApplicationContextRole;
import io.axoniq.axonserver.grpc.internal.ContextApplication;
import io.axoniq.axonserver.grpc.internal.ContextConfiguration;
import io.axoniq.axonserver.grpc.internal.ContextRole;
import io.axoniq.axonserver.grpc.internal.ContextUpdateConfirmation;
import io.axoniq.axonserver.grpc.internal.DeleteNode;
import io.axoniq.axonserver.grpc.internal.LoadBalanceStrategy;
import io.axoniq.axonserver.grpc.internal.NodeInfo;
import io.axoniq.axonserver.grpc.internal.NodeInfoWithLabel;
import io.axoniq.axonserver.grpc.internal.ProcessorLBStrategy;
import io.axoniq.axonserver.grpc.internal.User;
import io.axoniq.axonserver.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

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

/**
 * Service to orchestrate configuration changes. This service is executed on the leader of the _admin context.
 *
 * @author Marc Gathier
 */
@Component
class LocalRaftConfigService implements RaftConfigService {

    private final GrpcRaftController grpcRaftController;
    private final ContextController contextController;
    private final RaftGroupServiceFactory raftGroupServiceFactory;
    private final ApplicationController applicationController;
    private final MessagingPlatformConfiguration messagingPlatformConfiguration;
    private Logger logger = LoggerFactory.getLogger(LocalRaftConfigService.class);
    private final Predicate<String> contextNameValidation = new ContextNameValidation();
    private final CopyOnWriteArraySet<String> contextsInProgress = new CopyOnWriteArraySet<>();


    public LocalRaftConfigService(GrpcRaftController grpcRaftController, ContextController contextController,
                                  RaftGroupServiceFactory raftGroupServiceFactory,
                                  ApplicationController applicationController,
                                  MessagingPlatformConfiguration messagingPlatformConfiguration) {
        this.grpcRaftController = grpcRaftController;
        this.contextController = contextController;
        this.raftGroupServiceFactory = raftGroupServiceFactory;
        this.applicationController = applicationController;
        this.messagingPlatformConfiguration = messagingPlatformConfiguration;
    }

    @Override
    public void addNodeToContext(String context, String node) {
        logger.info("Add node request invoked for node: {} - and context: {}", node, context);
        RaftNode config = grpcRaftController.getRaftNode(getAdmin());
        Context contextDefinition = contextController.getContext(context);

        if (contextDefinition == null) {
            throw new MessagingPlatformException(ErrorCode.CONTEXT_NOT_FOUND,
                                                 String.format("Context %s not found", context));
        }
        ClusterNode clusterNode = contextController.getNode(node);
        if (clusterNode == null) {
            throw new MessagingPlatformException(ErrorCode.NO_SUCH_NODE, String.format("Node %s not found", node));
        }
        if (clusterNode.getContextNames().contains(context)) {
            return;
        }
        ContextConfiguration oldConfiguration = createContextConfigBuilder(contextDefinition).build();
        String nodeLabel = generateNodeLabel(node);
        Node raftNode = createNode(clusterNode, nodeLabel);

        ContextConfiguration contextConfiguration =
                ContextConfiguration.newBuilder(oldConfiguration).setPending(true)
                                    .build();

        getFuture(config.appendEntry(ContextConfiguration.class.getName(),
                                                           contextConfiguration.toByteArray()));

        try {
            raftGroupServiceFactory.getRaftGroupService(context)
                                   .addNodeToContext(context, raftNode)
                                   .whenComplete((result, throwable) ->
                                                         handleContextUpdateResult(context, node, "add", oldConfiguration, result, throwable));
        } catch( RuntimeException throwable) {
            logger.error("{}: Failed to add node {}", context, node, throwable);
            try {
                appendToAdmin(oldConfiguration.getClass().getName(), oldConfiguration.toByteArray());
            } catch (Exception second) {
                logger.debug("{}: Error while restoring old configuration in admin {}", context, second.getMessage());
            }
            throw throwable;
        }
    }

   private void handleContextUpdateResult(String context, String node, String action, ContextConfiguration oldConfiguration,
                                           ContextUpdateConfirmation result, Throwable throwable) {
        if (throwable == null) {
            if (!result.getSuccess()) {
                logger.error("{}: Failed to {} node {}: {}",
                             context,
                             action,
                             node,
                             result.getMessage());
            }
            ContextConfiguration updatedConfiguration = createContextConfiguration(context, result);
            try {
                sendToAdmin(updatedConfiguration.getClass().getName(), updatedConfiguration.toByteArray());
            } catch (Exception exception) {
                logger.warn("{}: Error sending updated configuration to admin {}", context, exception.getMessage());
            }
        } else {
            logger.error("{}: Failed to {} node {}", context, action, node, throwable);
            try {
                appendToAdmin(oldConfiguration.getClass().getName(), oldConfiguration.toByteArray());
            } catch (Exception adminException) {
                logger.debug("{}: Error while restoring old configuration in admin {}", context, adminException.getMessage());
            }
        }
    }

    private String generateNodeLabel(String node) {
        return node + "-" + UUID.randomUUID();
    }

    private Node createNode(ClusterNode clusterNode, String nodeLabel) {
        return Node.newBuilder().setNodeId(nodeLabel)
                   .setHost(clusterNode.getInternalHostName())
                   .setPort(clusterNode.getGrpcInternalPort())
                   .setNodeName(clusterNode.getName())
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
            contextController.getRemoteNodes().forEach(node -> raftGroupServiceFactory.getRaftGroupServiceForNode(node).deleteContext(context));
            raftGroupServiceFactory.getRaftGroupServiceForNode(this.messagingPlatformConfiguration.getName()).deleteContext(context);
            contextsInProgress.remove(context);
            return;
        }
        Collection<String> nodeNames = contextInAdmin.getNodeNames();
        @SuppressWarnings("unchecked")
        CompletableFuture<Void>[] workers = new CompletableFuture[nodeNames.size()];

        RaftNode config = grpcRaftController.getRaftNode(getAdmin());
        ContextConfiguration contextConfiguration = createContextConfigBuilder(contextInAdmin)
                                    .setPending(true)
                                    .build();

        getFuture(config.appendEntry(ContextConfiguration.class.getName(), contextConfiguration.toByteArray()));

        int nodeIdx = 0;
        Iterable<String> nodes = new HashSet<>(nodeNames);
        for (String name : nodes) {
            workers[nodeIdx] = raftGroupServiceFactory.getRaftGroupServiceForNode(name).deleteContext(context);
            workers[nodeIdx].thenAccept(r -> nodeNames.remove(name));
            nodeIdx++;
        }

        CompletableFuture.allOf(workers).whenComplete((result, exception) -> {
            ContextConfiguration.Builder updatedContextConfigurationBuilder =
                    ContextConfiguration.newBuilder()
                                        .setContext(context);
            if( exception != null) {
                logger.warn("{}: Could not delete context from {}", context, String.join(",", nodeNames), exception);

            contextInAdmin.getAllNodes().stream().filter(c-> nodeNames.contains(c.getClusterNode().getName()))
                          .forEach(c -> updatedContextConfigurationBuilder.addNodes(NodeInfoWithLabel.newBuilder()
                                                                                                     .setLabel(c.getClusterNodeLabel())
                          .setNode(c.getClusterNode().toNodeInfo())));
            }
            try {
                appendToAdmin(ContextConfiguration.class.getName(), updatedContextConfigurationBuilder.build().toByteArray());
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

        if (isAdmin(context) && contextDef.getNodeNames().size() == 1) {
            throw new MessagingPlatformException(ErrorCode.OTHER,
                                                 String.format("Cannot delete last node %s from admin context %s",
                                                               node,
                                                               context));
        }

        if( node.equals(raftGroupServiceFactory.getLeader(context))) {
            throw new MessagingPlatformException(ErrorCode.OTHER,
                                                 String.format("Cannot delete node %s from context %s as it is the current leader",
                                                               node,
                                                               context));
        }

        removeNodeFromContext(contextDef, raftGroupServiceFactory.getRaftGroupService(context), node, nodeLabel);
    }

    private CompletableFuture<Void> removeNodeFromContext(Context context,
                                                          RaftGroupService raftGroupService,
                                                          String node, String nodeLabel) {
        RaftNode config = grpcRaftController.getRaftNode(getAdmin());
        CompletableFuture<Void> removeDone = new CompletableFuture<>();
        ContextConfiguration oldConfiguration = createContextConfigBuilder(context)
                            .build();
        ContextConfiguration contextConfiguration =  ContextConfiguration.newBuilder(oldConfiguration)
                .setPending(true)
                .build();
        getFuture(config.appendEntry(ContextConfiguration.class.getName(), contextConfiguration.toByteArray()));
        try {
            raftGroupService.deleteNode(context.getName(), nodeLabel)
                                   .whenComplete((result, exception) -> {
                                       handleContextUpdateResult(context.getName(),
                                                                 node,
                                                                 "delete",
                                                                 oldConfiguration,
                                                                 result,
                                                                 exception);
                                       removeDone.complete(null);
                                   });
        } catch (Exception ex) {
            logger.error("{}: Failed to delete node {}", context, node, ex);
            try {
                appendToAdmin(ContextConfiguration.class.getName(), oldConfiguration.toByteArray());
            } catch (Exception second) {
                logger.debug("{}: Error while restoring configuration {}", context, second.getMessage());
            }
            removeDone.completeExceptionally(ex);
        }

        return removeDone;
    }

    @Override
    public void deleteNode(String name) {
        ClusterNode clusterNode = contextController.getNode(name);
        if( clusterNode == null) {
            logger.info("Delete Node: {} - Node not found.", name);
            return;
        }
        Set<ContextClusterNode> membersToDelete =
                clusterNode.getContexts()
                           .stream()
                           .filter(contextClusterNode -> contextClusterNode.getContext().getAllNodes().size() > 1)
                           .collect(Collectors.toSet());


        List<CompletableFuture<Void>> completableFutures = new ArrayList<>();
        membersToDelete.forEach(contextClusterNode ->
                                            completableFutures.add(removeNodeFromContext(contextClusterNode.getContext(),
                                                                                         raftGroupServiceFactory
                                                                                                 .getRaftGroupService(
                                                                                                         contextClusterNode.getContext().getName()),
                                                                                         contextClusterNode.getClusterNode().getName(),
                                                                                         contextClusterNode.getClusterNodeLabel()))
                               );

        getFuture(CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture[0]))
                         .thenCompose(r -> {
                             try {
                                 Thread.sleep((long) (grpcRaftController.electionTimeout() * 1.5));
                                 RaftNode config = grpcRaftController.getRaftNode(getAdmin());
                                 return config.appendEntry(DeleteNode.class.getName(),
                                                           DeleteNode.newBuilder().setNodeName(name).build()
                                                                     .toByteArray());
                             } catch (InterruptedException e) {
                                 Thread.currentThread().interrupt();
                                 return CompletableFuture.completedFuture(null);
                             }
                         }));
    }

    @Override
    public void addContext(String context, List<String> nodes) {
        if (!contextsInProgress.add(context)){
            throw new UnsupportedOperationException("The creation of the context is already in progress.");
        }
        Context contextDef = contextController.getContext(context);
        if( contextDef != null ) {
            contextsInProgress.remove(context);
            throw new MessagingPlatformException(ErrorCode.CONTEXT_EXISTS, String.format("Context %s already exists", context));
        }

        List<Node> raftNodes = new ArrayList<>();
        List<NodeInfoWithLabel> clusterNodes = new ArrayList<>();
        nodes.forEach(n -> {
            ClusterNode clusterNode = contextController.getNode(n);
            String nodeLabel = generateNodeLabel(n);
            raftNodes.add(createNode(clusterNode, nodeLabel));
            clusterNodes.add(NodeInfoWithLabel.newBuilder().setNode(clusterNode.toNodeInfo()).setLabel(nodeLabel)
                                              .build());
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
                }
            }));
    }

    @Override
    public void join(NodeInfo nodeInfo) {
        RaftNode adminNode = grpcRaftController.getRaftNode(getAdmin());
        if (!adminNode.isLeader()) {
            throw new MessagingPlatformException(ErrorCode.NODE_IS_REPLICA,
                                                 "Send join request to the leader of _admin context: " + adminNode
                                                         .getLeaderName());
        }
        List<String> contexts = nodeInfo.getContextsList().stream().map(ContextRole::getName).collect(Collectors
                                                                                                              .toList());
        if ((contexts.size() == 1) && contexts.get(0).equals(CONTEXT_NONE)) {
            logger.debug("join(): Joining to no contexts.");
            contexts.clear();
        }
        else if (contexts.isEmpty()) {
            logger.debug("join(): Joining to all contexts.");
            contexts = contextController.getContexts().map(Context::getName).collect(Collectors.toList());
        }
        else {
            logger.debug("join(): Joining to a specified set of contexts.");
        }

        String nodeLabel = generateNodeLabel(nodeInfo.getNodeName());
        Node node = Node.newBuilder().setNodeId(nodeLabel)
                        .setHost(nodeInfo.getInternalHostName())
                        .setPort(nodeInfo.getGrpcInternalPort())
                        .setNodeName(nodeInfo.getNodeName())
                        .build();
        contextController.addNode(nodeInfo);
        contexts.forEach(c -> {
            Context context = contextController.getContext(c);
            ContextConfiguration oldConfiguration = null;
            ContextConfiguration newContext;
            try {
                if (context != null) {
                    if(  context.getNodeNames().contains(nodeInfo.getNodeName())) {
                        logger.info("{}: Node {} is already member", c, node.getNodeName());
                    } else {
                        oldConfiguration = createContextConfigBuilder(context).build();
                        ContextConfiguration old = oldConfiguration;
                        ContextConfiguration pending = ContextConfiguration.newBuilder(old).setPending(true).build();

                        appendToAdmin(ContextConfiguration.class.getName(), pending.toByteArray());
                        raftGroupServiceFactory.getRaftGroupService(c)
                                               .addNodeToContext(c, node)
                                               .whenComplete((result, throwable) ->
                                                                     handleContextUpdateResult(c,
                                                                                               node.getNodeName(),
                                                                                               "add",
                                                                                               old,
                                                                                               result,
                                                                                               throwable));
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
                                           .whenComplete((result, throwable) -> {
                                               if (throwable == null) {
                                                   ContextConfiguration confirmConfiguration = ContextConfiguration
                                                           .newBuilder(newContext).setPending(false).build();
                                                   try {
                                                       sendToAdmin(confirmConfiguration.getClass().getName(), confirmConfiguration.toByteArray());
                                                   } catch (Exception second) {
                                                       logger.warn("{}: Error while restoring updated configuration in admin {}", c, second.getMessage());
                                                   }
                                               } else {
                                                   logger.warn("{}: Error while creating context", c, throwable);
                                                   try {
                                                       appendToAdmin(old.getClass().getName(), old.toByteArray());
                                                   } catch (Exception second) {
                                                       logger.debug("{}: Error while restoring old configuration in admin {}", c, second.getMessage());
                                                   }

                                               }
                                           });
                }
            } catch( Exception ex) {
                logger.warn("{}: Error while adding node {}", c, node.getNodeName(), ex);
                if( oldConfiguration != null) {
                    try {
                        appendToAdmin(ContextConfiguration.class.getName(), oldConfiguration.toByteArray());
                    } catch (Exception second) {
                        logger.debug("{}: Error while restoring old configuration {}", c, second.getMessage());
                    }
                }
            }
        });
    }

    private ContextConfiguration createContextConfiguration(String context, ContextUpdateConfirmation result) {
        ContextConfiguration.Builder builder = ContextConfiguration.newBuilder()
                                                                   .setContext(context)
                                                                   .setPending(result.getPending());

        result.getMembersList().forEach(contextMember -> {
            ClusterNode node = contextController.getNode(contextMember.getNodeName());
            if( node == null) {
                logger.warn("Could not find {} in admin", contextMember.getNodeName());
                node = new ClusterNode(contextMember.getNodeName(), null, contextMember.getHost(), null, contextMember.getPort(), null);
            }

            builder.addNodes(NodeInfoWithLabel.newBuilder().setNode(node.toNodeInfo()).setLabel(contextMember.getNodeId()));
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
                                .build();
    }

    @Override
    public void init(List<String> contexts) {
        for (String context : contexts) {
            if (!contextNameValidation.test(context)) {
                throw new MessagingPlatformException(ErrorCode.INVALID_CONTEXT_NAME, "Invalid context name: "+ context);
            }
        }
        logger.info("Initialization of this node with following contexts: {}", contexts);
        init(getAdmin());
        contexts.forEach(this::init);
    }

    private void init(String contextName){
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
                .addNodes(NodeInfoWithLabel.newBuilder().setNode(nodeInfo).setLabel(nodeLabelForContext))
                .build();
        RaftNode adminLeader = grpcRaftController.waitForLeader(grpcRaftController.getRaftGroup(getAdmin()));
        getFuture(adminLeader.appendEntry(ContextConfiguration.class.getName(), contextConfiguration.toByteArray()));
    }

    @Override
    public Application refreshToken(Application application)  {
        try {
            RaftNode config = grpcRaftController.getRaftNode(getAdmin());
            JpaApplication jpaApplication = applicationController.get(application.getName());

            String token = UUID.randomUUID().toString();
            Application updatedApplication = Application.newBuilder(ApplicationProtoConverter
                                                                            .createApplication(jpaApplication))
                                                        .setToken(applicationController.hash(token))
                                                        .setTokenPrefix(ApplicationController.tokenPrefix(token))
                                                        .build();
            return distributeApplication(config, updatedApplication, token);
        } catch (ApplicationNotFoundException notFound) {
            throw new MessagingPlatformException(ErrorCode.NO_SUCH_APPLICATION,
                                                 "Application not found");
        }
    }

    @Override
    public Application updateApplication(Application application)  {
        RaftNode config = grpcRaftController.getRaftNode(getAdmin());
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
        return distributeApplication(config, updatedApplication, token);
    }

    private Application distributeApplication(RaftNode config, Application updatedApplication,
                                              String token) {
        getFuture(config.appendEntry(Application.class.getName(), updatedApplication.toByteArray()));
        contextController.getContexts().forEach(c -> updateApplicationInGroup(updatedApplication, c));
        return Application.newBuilder(updatedApplication).setToken(token).build();
    }

    private void updateApplicationInGroup(Application updatedApplication, Context c) {
        try {
            ApplicationContextRole roles = getRolesPerContext(updatedApplication,
                                                              c.getName());
            ContextApplication contextApplication =
                    ContextApplication.newBuilder()
                                      .setContext(c.getName())
                                      .setName(updatedApplication.getName())
                                      .setHashedToken(updatedApplication.getToken())
                                      .setTokenPrefix(updatedApplication
                                                              .getTokenPrefix())
                                      .addAllRoles(roles == null || roles.getRolesCount() == 0 ?
                                                           Collections.emptyList() : roles.getRolesList()).build();

            raftGroupServiceFactory.getRaftGroupService(c.getName())
                                   .updateApplication(contextApplication).get();
        } catch (Exception ex) {
            logger.warn("Failed to update application in context {}", c.getName(), ex);
        }
    }

    @Override
    public void updateUser(User request)  {
        RaftNode config = grpcRaftController.getRaftNode(getAdmin());
        getFuture(config.appendEntry(User.class.getName(), request.toByteArray()));
    }

    private ApplicationContextRole getRolesPerContext(Application application, String name) {
        for (ApplicationContextRole applicationContextRole : application.getRolesPerContextList()) {
            if (name.equals(applicationContextRole.getContext())) {
                return applicationContextRole;
            }
        }
        return null;
    }

    @Override
    public void updateLoadBalancingStrategy(LoadBalanceStrategy loadBalancingStrategy) {
        RaftNode config = grpcRaftController.getRaftNode(getAdmin());
        getFuture(config.appendEntry(LoadBalanceStrategy.class.getName(), loadBalancingStrategy.toByteArray()));
        contextController.getContexts()
                        .filter(c -> !isAdmin(c.getName()))
                        .forEach(c -> raftGroupServiceFactory.getRaftGroupService(c.getName())
                                                                                    .updateLoadBalancingStrategy(c.getName(),
                                                                                                                 loadBalancingStrategy));
    }


    @Override
    public void updateProcessorLoadBalancing(ProcessorLBStrategy processorLBStrategy) {
        RaftNode config = grpcRaftController.getRaftNode(getAdmin());
        getFuture(config.appendEntry(ProcessorLBStrategy.class.getName(), processorLBStrategy.toByteArray()));
        raftGroupServiceFactory.getRaftGroupService(processorLBStrategy.getContext())
                                                                 .updateProcessorLoadBalancing(processorLBStrategy
                                                                                                       .getContext(),
                                                                                               processorLBStrategy);
    }

    private ContextConfiguration.Builder createContextConfigBuilder(Context context) {
        ContextConfiguration.Builder groupConfigurationBuilder = ContextConfiguration.newBuilder()
                                                                                     .setContext(context.getName());
        context.getAllNodes().forEach(n -> groupConfigurationBuilder
                .setPending(context.isChangePending())
                .addNodes(NodeInfoWithLabel.newBuilder()
                                           .setNode(n.getClusterNode().toNodeInfo())
                                           .setLabel(n.getClusterNodeLabel())
                                           .build()));
        return groupConfigurationBuilder;
    }

    @Override
    public void deleteUser(User user)  {
        RaftNode config = grpcRaftController.getRaftNode(getAdmin());
        getFuture(config.appendEntry(DELETE_USER, user.toByteArray()));
    }

    @Override
    public void deleteApplication(Application application)  {
        RaftNode config = grpcRaftController.getRaftNode(getAdmin());
        getFuture(config.appendEntry(DELETE_APPLICATION, application.toByteArray()));
        contextController.getContexts().forEach(c ->
                              raftGroupServiceFactory.getRaftGroupService(c.getName())
                                                     .updateApplication(ContextApplication.newBuilder()
                                                                                          .setContext(c.getName())
                                                                                          .setName(application.getName())
                                                                                          .build()));
    }

    @Override
    public void deleteLoadBalancingStrategy(LoadBalanceStrategy loadBalancingStrategy) {
        RaftNode config = grpcRaftController.getRaftNode(getAdmin());
        getFuture(config.appendEntry(DELETE_LOAD_BALANCING_STRATEGY, loadBalancingStrategy.toByteArray()));
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
     * @param leaderConfirmation the event
     */
    @EventListener
    public void on(ClusterEvents.LeaderConfirmation leaderConfirmation) {
        checkPendingChanges(leaderConfirmation.getContext());
    }

    /**
     * Event handler for {@link io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents.BecomeLeader} events.
     * Checks if there were configuration changes pending when the leader changed, and if so, removes the flag.
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
            if( admin.isLeader()) {
                Context context = contextController.getContext(contextName);
                if( context != null && context.isChangePending()) {
                    ContextConfiguration configuration = createContextConfigBuilder(context).setPending(false).build();
                    admin.appendEntry(ContextConfiguration.class.getName(), configuration.toByteArray());
                }
            }
        } catch(MessagingPlatformException ex) {
            if( !ErrorCode.CONTEXT_NOT_FOUND.equals(ex.getErrorCode())) {
                logger.warn("Error checking pending changes", ex);
            }
        }
    }
}
