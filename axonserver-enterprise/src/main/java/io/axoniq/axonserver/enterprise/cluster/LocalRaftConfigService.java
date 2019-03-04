package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.access.application.ApplicationController;
import io.axoniq.axonserver.access.application.ApplicationNotFoundException;
import io.axoniq.axonserver.access.application.JpaApplication;
import io.axoniq.axonserver.cluster.RaftGroup;
import io.axoniq.axonserver.cluster.RaftNode;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.context.ContextController;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.axoniq.axonserver.RaftAdminGroup.getAdmin;
import static io.axoniq.axonserver.RaftAdminGroup.isAdmin;
import static io.axoniq.axonserver.enterprise.logconsumer.DeleteApplicationConsumer.DELETE_APPLICATION;
import static io.axoniq.axonserver.enterprise.logconsumer.DeleteLoadBalancingStrategyConsumer.DELETE_LOAD_BALANCING_STRATEGY;
import static io.axoniq.axonserver.enterprise.logconsumer.DeleteUserConsumer.DELETE_USER;

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

        if (contextController.getContext(context) == null) {
            throw new MessagingPlatformException(ErrorCode.CONTEXT_NOT_FOUND,
                                                 String.format("Context %s not found", node, context));
        }
        ClusterNode clusterNode = contextController.getNode(node);
        if (clusterNode == null) {
            throw new MessagingPlatformException(ErrorCode.NO_SUCH_NODE, String.format("Node %s not found", node));
        }
        if (clusterNode.getContextNames().contains(context)) {
            throw new MessagingPlatformException(ErrorCode.ALREADY_MEMBER_OF_CLUSTER,
                                                 String.format("Node %s already member of context %s", node, context));
        }
        String nodeLabel = generateNodeLabel(node);
        Node raftNode = createNode(clusterNode, nodeLabel);
        try {
            raftGroupServiceFactory.getRaftGroupService(context).addNodeToContext(context, raftNode).thenCompose(
                    r -> {
                        NodeInfoWithLabel newMember = NodeInfoWithLabel.newBuilder().setLabel(nodeLabel).setNode(
                                clusterNode.toNodeInfo()).build();
                        ContextConfiguration contextConfiguration =
                                ContextConfiguration.newBuilder()
                                                    .setContext(context)
                                                    .addAllNodes(
                                                            Stream.concat(nodes(context), Stream.of(newMember))
                                                                  .collect(Collectors.toList()))
                                                    .build();
                        return config.appendEntry(ContextConfiguration.class.getName(),
                                                  contextConfiguration.toByteArray());
                    }).get(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new MessagingPlatformException(ErrorCode.OTHER, e.getMessage(), e);
        } catch (ExecutionException e) {
            logger.warn("{}: Failed to add node {}", context, node, e);
            throw new MessagingPlatformException(ErrorCode.OTHER, "Failed to execute", e.getCause());
        } catch (TimeoutException e) {
            throw new MessagingPlatformException(ErrorCode.OTHER, "Timeout while adding node to context", e);
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

    private Stream<NodeInfoWithLabel> nodes(String context) {
        Context ctx = contextController.getContext(context);
        if (ctx == null) {
            return Stream.empty();
        }
        return ctx.getAllNodes()
                  .stream()
                  .map(ccn -> NodeInfoWithLabel.newBuilder().setNode(ccn.getClusterNode().toNodeInfo())
                                               .setLabel(ccn.getClusterNodeLabel()).build());
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
        int nodeIdx = 0;
        Iterable<String> nodes = new HashSet<>(nodeNames);
        for (String name : nodes) {
            workers[nodeIdx] = raftGroupServiceFactory.getRaftGroupServiceForNode(name).deleteContext(context);
            workers[nodeIdx].thenAccept(r -> nodeNames.remove(name));
            nodeIdx++;
        }
        try {
            CompletableFuture.allOf(workers).get(10, TimeUnit.SECONDS);
            RaftNode config = grpcRaftController.getRaftNode(getAdmin());
            ContextConfiguration contextConfiguration =
                    ContextConfiguration.newBuilder()
                                        .setContext(context)
                                        .build();
            config.appendEntry(contextConfiguration.getClass().getName(), contextConfiguration.toByteArray()).get();
            if (!nodeNames.isEmpty()) {
                throw new MessagingPlatformException(ErrorCode.OTHER,
                                                     context + ": Could not delete context from " + String
                                                             .join(",", nodeNames));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new MessagingPlatformException(ErrorCode.OTHER, e.getMessage(), e);
        } catch (ExecutionException e) {
            logger.warn("Failed to delete context {}", context, e);
            throw new MessagingPlatformException(ErrorCode.OTHER, "Failed to execute", e.getCause());
        } catch (TimeoutException e) {
            throw new MessagingPlatformException(ErrorCode.OTHER, "Timeout while deleting context", e);
        }
    }


    @Override
    public void deleteNodeFromContext(String context, String node) {
        Context contextDef = contextController.getContext(context);
        String nodeLabel = contextDef.getNodeLabel(node);

        if (!contextDef.getNodeNames().contains(node)) {
            throw new MessagingPlatformException(ErrorCode.OTHER,
                                                 String.format("Node %s not found in context %s", node, context));
        }

        if (isAdmin(context) && contextDef.getNodeNames().size() == 1) {
            throw new MessagingPlatformException(ErrorCode.OTHER,
                                                 String.format("Cannot delete last node %s from admin context %s",
                                                               node,
                                                               context));
        }

        try {
            removeNodeFromContext(context, node, nodeLabel).get(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new MessagingPlatformException(ErrorCode.OTHER, e.getMessage(), e);
        } catch (ExecutionException e) {
            logger.warn("{}: Failed to delete node {}", context, node, e);
            throw new MessagingPlatformException(ErrorCode.OTHER, "Failed to execute", e.getCause());
        } catch (TimeoutException e) {
            throw new MessagingPlatformException(ErrorCode.OTHER, "Timeout while deleting node from context", e);
        }
    }

    private CompletableFuture<Void> removeNodeFromContext(String context, String node, String nodeLabel) {
        RaftNode config = grpcRaftController.getRaftNode(getAdmin());
        return raftGroupServiceFactory.getRaftGroupService(context).deleteNode(context, nodeLabel).thenCompose(r -> {
            ContextConfiguration contextConfiguration =
                    ContextConfiguration.newBuilder()
                                        .setContext(context)
                                        .addAllNodes(nodes(context)
                                                             .filter(n -> !n.getNode().getNodeName().equals(node))
                                                             .collect(Collectors.toList()))
                                        .build();
            return config.appendEntry(ContextConfiguration.class.getName(),
                                      contextConfiguration.toByteArray());
        });
    }

    @Override
    public void deleteNode(String name) {
        ClusterNode clusterNode = contextController.getNode(name);
        Set<ContextClusterNode> membersToDelete =
                clusterNode.getContexts()
                           .stream()
                           .filter(contextClusterNode -> contextClusterNode.getContext().getAllNodes().size() > 1)
                           .collect(Collectors.toSet());

        List<CompletableFuture<Void>> completableFutures =
                membersToDelete.stream()
                               .map(contextClusterNode ->
                                            removeNodeFromContext( contextClusterNode.getContext().getName(),
                                                                   contextClusterNode.getClusterNode().getName(),
                                                                   contextClusterNode.getClusterNodeLabel())
                               ).collect(Collectors.toList());

        try {
            CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture[0]))
                             .thenCompose(r -> {
                                 RaftNode config = grpcRaftController.getRaftNode(getAdmin());
                                 return config.appendEntry(DeleteNode.class.getName(),
                                                           DeleteNode.newBuilder().setNodeName(name).build()
                                                                     .toByteArray());
                             })
                             .get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new MessagingPlatformException(ErrorCode.OTHER, e.getMessage(), e);
        } catch (ExecutionException e) {
            logger.warn("Failed to delete node {}", name, e);
            throw new MessagingPlatformException(ErrorCode.OTHER, "Failed to execute", e.getCause());
        }
    }

    @Override
    public void addContext(String context, List<String> nodes) {
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

        try {
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
                                   }).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new MessagingPlatformException(ErrorCode.OTHER, e.getMessage(), e);
        } catch (ExecutionException e) {
            throw new MessagingPlatformException(ErrorCode.OTHER, e.getCause().getMessage(), e.getCause());
        }
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
        if (contexts.isEmpty()) {
            contexts = contextController.getContexts().map(Context::getName).collect(Collectors.toList());
        }

        String nodeLabel = generateNodeLabel(nodeInfo.getNodeName());
        Node node = Node.newBuilder().setNodeId(nodeLabel)
                        .setHost(nodeInfo.getInternalHostName())
                        .setPort(nodeInfo.getGrpcInternalPort())
                        .setNodeName(nodeInfo.getNodeName())
                        .build();
        List<CompletableFuture<Void>> completableFutures = new ArrayList<>();
        contextController.addNode(nodeInfo);
        contexts.forEach(c -> {

            Context context = contextController.getContext(c);
            if (context != null) {
                completableFutures.add(raftGroupServiceFactory.getRaftGroupService(c).addNodeToContext(c, node)
                                                              .thenAccept(v -> adminNode
                                                                      .appendEntry(ContextConfiguration.class.getName(),
                                                                                   createContextConfigBuilder(c)
                                                                                           .addNodes(
                                                                                                   NodeInfoWithLabel
                                                                                                           .newBuilder()
                                                                                                           .setLabel(
                                                                                                                   nodeLabel)
                                                                                                           .setNode(
                                                                                                                   nodeInfo)
                                                                                                           .build())
                                                                                           .build().toByteArray())));
            } else {
                completableFutures.add(raftGroupServiceFactory.getRaftGroupServiceForNode(ClusterNode.from(nodeInfo))
                                                              .initContext(c, Collections.singletonList(node))
                                                              .thenCompose(r -> {
                                                                  ContextConfiguration.Builder groupConfigurationBuilder = ContextConfiguration
                                                                          .newBuilder()
                                                                          .setContext(c);
                                                                  return adminNode
                                                                          .appendEntry(ContextConfiguration.class
                                                                                               .getName(),
                                                                                       groupConfigurationBuilder
                                                                                               .addNodes(
                                                                                                       NodeInfoWithLabel
                                                                                                               .newBuilder()
                                                                                                               .setLabel(
                                                                                                                       nodeLabel)
                                                                                                               .setNode(
                                                                                                                       nodeInfo)
                                                                                                               .build())
                                                                                               .build().toByteArray());
                                                              }));
            }
        });
        try {
            CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture[0])).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new MessagingPlatformException(ErrorCode.OTHER, e.getMessage(), e);
        } catch (ExecutionException e) {
            throw new MessagingPlatformException(ErrorCode.OTHER, e.getCause().getMessage(), e.getCause());
        }
    }

    @Override
    public void init(List<String> contexts) {
        try {
            String adminLabel = generateNodeLabel(messagingPlatformConfiguration.getName());
            RaftGroup configGroup = grpcRaftController.initRaftGroup(getAdmin(),
                                                                     adminLabel,
                                                                     messagingPlatformConfiguration.getName());
            RaftNode leader = grpcRaftController.waitForLeader(configGroup);
            Node me = Node.newBuilder().setNodeId(adminLabel)
                          .setHost(messagingPlatformConfiguration.getFullyQualifiedInternalHostname())
                          .setPort(messagingPlatformConfiguration.getInternalPort())
                          .setNodeName(messagingPlatformConfiguration.getName())
                          .build();

            leader.addNode(me).get();
            NodeInfo nodeInfo = NodeInfo.newBuilder()
                                        .setGrpcInternalPort(messagingPlatformConfiguration.getInternalPort())
                                        .setNodeName(messagingPlatformConfiguration.getName())
                                        .setInternalHostName(messagingPlatformConfiguration
                                                                     .getFullyQualifiedInternalHostname())
                                        .setGrpcPort(messagingPlatformConfiguration.getPort())
                                        .setHttpPort(messagingPlatformConfiguration.getHttpPort())
                                        .setHostName(messagingPlatformConfiguration.getFullyQualifiedHostname())
                                        .build();
            ContextConfiguration contextConfiguration = ContextConfiguration.newBuilder()
                                                                            .setContext(getAdmin())
                                                                            .addNodes(NodeInfoWithLabel.newBuilder()
                                                                                                       .setNode(nodeInfo)
                                                                                                       .setLabel(
                                                                                                               adminLabel))
                                                                            .build();
            leader.appendEntry(ContextConfiguration.class.getName(), contextConfiguration.toByteArray()).get();
            List<CompletableFuture<Void>> completableFutures = new ArrayList<>();
            contexts.forEach(c -> {
                String contextLabel = generateNodeLabel(messagingPlatformConfiguration.getName());
                RaftGroup group = grpcRaftController.initRaftGroup(c,
                                                                   contextLabel,
                                                                   messagingPlatformConfiguration.getName());
                RaftNode groupLeader = grpcRaftController.waitForLeader(group);
                Node contextMe = Node.newBuilder().setNodeId(contextLabel)
                                     .setHost(messagingPlatformConfiguration.getFullyQualifiedInternalHostname())
                                     .setPort(messagingPlatformConfiguration.getInternalPort())
                                     .setNodeName(messagingPlatformConfiguration.getName())
                                     .build();

                completableFutures.add(groupLeader.addNode(contextMe).thenCompose(r -> {
                    ContextConfiguration groupConfiguration = ContextConfiguration.newBuilder()
                                                                                  .setContext(c)
                                                                                  .addNodes(NodeInfoWithLabel
                                                                                                    .newBuilder()
                                                                                                    .setNode(nodeInfo)
                                                                                                    .setLabel(
                                                                                                            contextLabel))
                                                                                  .build();
                    return leader.appendEntry(ContextConfiguration.class.getName(), groupConfiguration.toByteArray());
                }));
            });

            CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture[0])).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new MessagingPlatformException(ErrorCode.OTHER, e.getMessage(), e);
        } catch (ExecutionException e) {
            throw new MessagingPlatformException(ErrorCode.OTHER, e.getCause().getMessage(), e.getCause());
        }
    }

    @Override
    public CompletableFuture<Application> refreshToken(Application application) {
        CompletableFuture<Application> result = new CompletableFuture<>();
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
            result.completeExceptionally(new MessagingPlatformException(ErrorCode.NO_SUCH_APPLICATION,
                                                                        "Application not found"));
        } catch (Exception other) {
            result.completeExceptionally(new MessagingPlatformException(ErrorCode.OTHER, other.getMessage()));
        }
        return result;
    }

    @Override
    public CompletableFuture<Application> updateApplication(Application application) {
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

    private CompletableFuture<Application> distributeApplication(RaftNode config, Application updatedApplication,
                                                                 String token) {
        return config.appendEntry(Application.class.getName(), updatedApplication.toByteArray())
                     .thenApply(done -> {
                                    contextController.getContexts()
                                                     .forEach(c -> updateApplicationInGroup(updatedApplication, c));
                                    return Application.newBuilder(updatedApplication).setToken(token).build();
                                }
                     );
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
    public CompletableFuture<Void> updateUser(User request) {
        RaftNode config = grpcRaftController.getRaftNode(getAdmin());
        return config.appendEntry(User.class.getName(), request.toByteArray());
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
    public CompletableFuture<Void> updateLoadBalancingStrategy(LoadBalanceStrategy loadBalancingStrategy) {
        RaftNode config = grpcRaftController.getRaftNode(getAdmin());
        CompletableFuture<Void> result = new CompletableFuture<>();
        config.appendEntry(LoadBalanceStrategy.class.getName(), loadBalancingStrategy.toByteArray())
              .whenComplete(
                      (done, throwable) -> {
                          if (throwable != null) {
                              logger.warn("_admin: Failed to add load balancing strategy", throwable);
                              result.completeExceptionally(throwable);
                          } else {
                              result.complete(null);
                              contextController.getContexts()
                                               .filter(c -> !isAdmin(c.getName()))
                                               .forEach(c -> raftGroupServiceFactory.getRaftGroupService(c.getName())
                                                                                    .updateLoadBalancingStrategy(c.getName(),
                                                                                                                 loadBalancingStrategy));
                          }
                      }
              );
        return result;
    }


    @Override
    public CompletableFuture<Void> updateProcessorLoadBalancing(ProcessorLBStrategy processorLBStrategy) {
        RaftNode config = grpcRaftController.getRaftNode(getAdmin());
        return config.appendEntry(ProcessorLBStrategy.class.getName(), processorLBStrategy.toByteArray())
                     .thenCompose(r ->
                                          raftGroupServiceFactory.getRaftGroupService(processorLBStrategy.getContext())
                                                                 .updateProcessorLoadBalancing(processorLBStrategy
                                                                                                       .getContext(),
                                                                                               processorLBStrategy));
    }

    private ContextConfiguration.Builder createContextConfigBuilder(String c) {
        ContextConfiguration.Builder groupConfigurationBuilder = ContextConfiguration.newBuilder()
                                                                                     .setContext(c);
        contextController.getContext(c).getAllNodes().forEach(n -> groupConfigurationBuilder
                .addNodes(NodeInfoWithLabel.newBuilder()
                                           .setNode(n.getClusterNode().toNodeInfo())
                                           .setLabel(n.getClusterNodeLabel())
                                           .build()));
        return groupConfigurationBuilder;
    }

    @Override
    public CompletableFuture<Void> deleteUser(User user) {
        RaftNode config = grpcRaftController.getRaftNode(getAdmin());
        return config.appendEntry(DELETE_USER, user.toByteArray());
    }

    @Override
    public CompletableFuture<Void> deleteApplication(Application application) {
        RaftNode config = grpcRaftController.getRaftNode(getAdmin());
        List<CompletableFuture<Void>> completableFutures = new ArrayList<>();
        config.appendEntry(DELETE_APPLICATION, application.toByteArray())
              .thenAccept(done -> contextController
                      .getContexts().forEach(c -> completableFutures.add(
                              raftGroupServiceFactory.getRaftGroupService(c.getName())
                                                     .updateApplication(ContextApplication.newBuilder()
                                                                                          .setContext(c.getName())
                                                                                          .setName(application.getName())
                                                                                          .build())))
              );
        return CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture[0]));
    }

    @Override
    public CompletableFuture<Void> deleteLoadBalancingStrategy(LoadBalanceStrategy loadBalancingStrategy) {
        RaftNode config = grpcRaftController.getRaftNode(getAdmin());
        CompletableFuture<Void> result = new CompletableFuture<>();
        config.appendEntry(DELETE_LOAD_BALANCING_STRATEGY, loadBalancingStrategy.toByteArray())
              .whenComplete(
                      (done, throwable) -> {
                          if (throwable != null) {
                              logger.warn("_admin: Failed to delete application", throwable);
                              result.completeExceptionally(throwable);
                          } else {
                              result.complete(null);
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
              );
        return result;
    }
}
