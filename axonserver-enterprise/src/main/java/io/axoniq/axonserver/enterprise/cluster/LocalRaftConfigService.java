package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.access.application.ApplicationController;
import io.axoniq.axonserver.access.application.ApplicationNotFoundException;
import io.axoniq.axonserver.access.application.JpaApplication;
import io.axoniq.axonserver.cluster.RaftNode;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
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
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
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
            throw new MessagingPlatformException(ErrorCode.ALREADY_MEMBER_OF_CLUSTER,
                                                 String.format("Node %s already member of context %s", node, context));
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
            appendToAdmin(oldConfiguration.getClass().getName(),
                          oldConfiguration.toByteArray());
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
            appendToAdmin(updatedConfiguration.getClass().getName(),
                          updatedConfiguration.toByteArray());
        } else {
            logger.error("{}: Failed to {} node {}", context, action, node, throwable);
            appendToAdmin(oldConfiguration.getClass().getName(),
                          oldConfiguration.toByteArray());
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
        if (isAdmin(context)) {
            throw new MessagingPlatformException(ErrorCode.CANNOT_DELETE_INTERNAL_CONTEXT,
                                                 String.format("Deletion of internal context %s not allowed", context));
        }

        Context contextInAdmin = contextController.getContext(context);
        if (contextInAdmin == null) {
            throw new MessagingPlatformException(ErrorCode.CONTEXT_NOT_FOUND,
                                                 String.format("Context %s not found", context));
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

            appendToAdmin(ContextConfiguration.class.getName(), updatedContextConfigurationBuilder.build().toByteArray());
        });
    }


    @Override
    public void deleteNodeFromContext(String context, String node) {
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

        ;
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
            appendToAdmin(oldConfiguration.getClass().getName(),
                          oldConfiguration.toByteArray());
            removeDone.completeExceptionally(ex);
        }

        return removeDone;
    }

    @Override
    public void deleteNode(String name) {
        ClusterNode clusterNode = contextController.getNode(name);
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
        Context contextDef = contextController.getContext(context);
        if( contextDef != null ) {
            throw new MessagingPlatformException(ErrorCode.CONTEXT_EXISTS, String.format("Context %s already exists", context));
        }

        RaftNode config = grpcRaftController.getRaftNode(getAdmin());
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
                                   .thenCompose(r -> {
                                       ContextConfiguration contextConfiguration = ContextConfiguration.newBuilder()
                                                                                                       .setContext(
                                                                                                               context)
                                                                                                       .addAllNodes(
                                                                                                               clusterNodes)
                                                                                                       .build();
                                       return config.appendEntry(ContextConfiguration.class.getName(),
                                                                 contextConfiguration.toByteArray());
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
                    oldConfiguration = createContextConfigBuilder(context).build();
                    ContextConfiguration old = oldConfiguration;
                    ContextConfiguration pending = ContextConfiguration.newBuilder(old).setPending(true).build();

                    getFuture(adminNode.appendEntry(ContextConfiguration.class.getName(), pending.toByteArray()));
                    raftGroupServiceFactory.getRaftGroupService(c)
                                           .addNodeToContext(c, node)
                                           .whenComplete((result, throwable) ->
                                                                 handleContextUpdateResult(c, node.getNodeName(), "add", old, result, throwable));
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
                                                   appendToAdmin(confirmConfiguration.getClass().getName(), confirmConfiguration.toByteArray());
                                               } else {
                                                   logger.warn("{}: Error while creating context", c, throwable);
                                                   appendToAdmin(old.getClass().getName(), old.toByteArray());

                                               }
                                           });
                }
            } catch( Exception ex) {
                logger.warn("{}: Error while adding node {}", c, node.getNodeName(), ex);
                if( oldConfiguration != null) {
                    appendToAdmin(ContextConfiguration.class.getName(), oldConfiguration.toByteArray());
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
        try {
            raftGroupServiceFactory.getRaftGroupService(getAdmin()).appendEntry(
                    getAdmin(),
                    name,
                    bytes).whenComplete((result, throwable) -> {
                if (throwable != null) {
                    logger.warn("{}: append entry {} failed", getAdmin(), name, throwable);
                }
            });
        } catch(Exception ex) {
            logger.warn("{}: append entry {} failed", getAdmin(), name, ex);
        }
    }

    private NodeInfoWithLabel newNodeInfoWithLabel(String nodeLabel, NodeInfo nodeInfo) {
        return NodeInfoWithLabel.newBuilder()
                                .setLabel(nodeLabel)
                                .setNode(nodeInfo)
                                .build();
    }

    @Override
    public void init(List<String> contexts) {
        if (!grpcRaftController.getContexts().isEmpty()){
            throw new MessagingPlatformException(ErrorCode.ALREADY_MEMBER_OF_CLUSTER,
                    "This node is already part of a cluster and cannot be initialized again.");
        }
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
}
