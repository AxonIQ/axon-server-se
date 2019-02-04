package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.cluster.RaftGroup;
import io.axoniq.axonserver.cluster.RaftNode;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.context.ContextController;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.enterprise.jpa.Context;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.internal.Application;
import io.axoniq.axonserver.grpc.internal.ApplicationContextRole;
import io.axoniq.axonserver.grpc.internal.ContextConfiguration;
import io.axoniq.axonserver.grpc.internal.ContextRole;
import io.axoniq.axonserver.grpc.internal.LoadBalanceStrategy;
import io.axoniq.axonserver.grpc.internal.NodeInfo;
import io.axoniq.axonserver.grpc.internal.NodeInfoWithLabel;
import io.axoniq.axonserver.grpc.internal.ProcessorLBStrategy;
import io.axoniq.axonserver.grpc.internal.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.axoniq.axonserver.enterprise.logconsumer.DeleteApplicationConsumer.DELETE_APPLICATION;
import static io.axoniq.axonserver.enterprise.logconsumer.DeleteLoadBalancingStrategyConsumer.DELETE_LOAD_BALANCING_STRATEGY;
import static io.axoniq.axonserver.enterprise.logconsumer.DeleteUserConsumer.DELETE_USER;

/**
 * Author: marc
 */
@Component
class LocalRaftConfigService implements RaftConfigService {

    private final GrpcRaftController grpcRaftController;
    private final ContextController contextController;
    private final RaftGroupServiceFactory raftGroupServiceFactory;
    private final MessagingPlatformConfiguration messagingPlatformConfiguration;
    private Logger logger = LoggerFactory.getLogger(LocalRaftConfigService.class);

    public LocalRaftConfigService(GrpcRaftController grpcRaftController, ContextController contextController,
                                  RaftGroupServiceFactory raftGroupServiceFactory,
                                  MessagingPlatformConfiguration messagingPlatformConfiguration) {
        this.grpcRaftController = grpcRaftController;
        this.contextController = contextController;
        this.raftGroupServiceFactory = raftGroupServiceFactory;
        this.messagingPlatformConfiguration = messagingPlatformConfiguration;
    }

    @Override
    public void addNodeToContext(String context, String node) {
        RaftNode config = grpcRaftController.getRaftNode(GrpcRaftController.ADMIN_GROUP);

        ClusterNode clusterNode = contextController.getNode(node);
        String nodeLabel = generateNodeLabel(node);
        Node raftNode = createNode(clusterNode, nodeLabel);
        raftGroupServiceFactory.getRaftGroupService(context).addNodeToContext(context, raftNode).thenApply(
                r -> {
                    NodeInfoWithLabel newMember = NodeInfoWithLabel.newBuilder().setLabel(nodeLabel).setNode(clusterNode.toNodeInfo()).build();
                    ContextConfiguration contextConfiguration =
                            ContextConfiguration.newBuilder()
                                                .setContext(context)
                                                .addAllNodes(
                                                        Stream.concat(nodes(context),Stream.of(newMember))
                                                              .collect(Collectors.toList()))
                                                .build();
                    config.appendEntry(ContextConfiguration.class.getName(),
                                       contextConfiguration.toByteArray());
                    return r;
                });
    }

    private String generateNodeLabel(String node) {
        return node + "-" + UUID.randomUUID();
    }

    private Node createNode(ClusterNode clusterNode, String nodeLabel) {
        return Node.newBuilder().setNodeId(nodeLabel).setHost(clusterNode.getInternalHostName()).setPort(clusterNode.getGrpcInternalPort()).build();
    }

    private Stream<NodeInfoWithLabel> nodes(String context) {
        return contextController.getContext(context).getAllNodes()
                                .stream()
                                .map( ccn -> NodeInfoWithLabel.newBuilder().setNode(ccn.getClusterNode().toNodeInfo()).setLabel(ccn.getClusterNodeLabel()).build());
    }

    @Override
    public void deleteContext(String context) {
        Collection<String> nodeNames = contextController.getContext(context).getNodeNames();
        CompletableFuture<Void>[] workers = new CompletableFuture[nodeNames.size()];
        int nodeIdx = 0;
        for( String name : nodeNames) {
            workers[nodeIdx] = raftGroupServiceFactory.getRaftGroupServiceForNode(name).deleteContext(context);
            workers[nodeIdx].whenComplete((r,throwable) -> {
                if( throwable == null) nodeNames.remove(name);
            });
            nodeIdx++;
        }
        try {
            CompletableFuture.allOf(workers).get(10, TimeUnit.SECONDS);
            RaftNode config = grpcRaftController.getRaftNode(GrpcRaftController.ADMIN_GROUP);
            ContextConfiguration contextConfiguration =
                        ContextConfiguration.newBuilder()
                                            .setContext(context)
                                            .build();
            config.appendEntry(contextConfiguration.getClass().getName(), contextConfiguration.toByteArray()).get();
            if( nodeNames.isEmpty()) {
                throw new MessagingPlatformException(ErrorCode.OTHER, context + ": Could not delete context from " + String.join(",", nodeNames));
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            throw new MessagingPlatformException(ErrorCode.OTHER, "Failed to execute", e.getCause());
        } catch (TimeoutException e) {
            throw new MessagingPlatformException(ErrorCode.OTHER, "Timeout while deleting context", e);
        }
    }


    @Override
    public void deleteNodeFromContext(String context, String node) {
        RaftNode config = grpcRaftController.getRaftNode(GrpcRaftController.ADMIN_GROUP);
        Context contextDef = contextController.getContext(context);
        String nodeLabel = contextDef.getNodeLabel(node);

        raftGroupServiceFactory.getRaftGroupService(context).deleteNode(context, nodeLabel).thenApply(r -> {
                ContextConfiguration contextConfiguration =
                        ContextConfiguration.newBuilder()
                                            .setContext(context)
                                            .addAllNodes(nodes(context)
                                                                 .filter(n -> !n.getNode().getNodeName().equals(node))
                                                                 .collect(Collectors.toList()))
                                            .build();
                config.appendEntry(ContextConfiguration.class.getName(),
                                   contextConfiguration.toByteArray());
                return r;
        }).exceptionally(t -> {
            logger.warn("{}: Delete node {} failed", context, node, t);
            return null;
        });
    }

    @Override
    public void addContext(String context, List<String> nodes) {
        RaftNode config = grpcRaftController.getRaftNode(GrpcRaftController.ADMIN_GROUP);
        List<Node> raftNodes = new ArrayList<>();
        List<NodeInfoWithLabel> clusterNodes = new ArrayList<>();
        nodes.forEach(n -> {
            ClusterNode clusterNode = contextController.getNode(n);
            String nodeLabel = generateNodeLabel(n);
            raftNodes.add( createNode(clusterNode, nodeLabel));
            clusterNodes.add( NodeInfoWithLabel.newBuilder().setNode(clusterNode.toNodeInfo()).setLabel(nodeLabel).build());

        });
        Node target = raftNodes.get(0);

        raftGroupServiceFactory.getRaftGroupServiceForNode(target.getNodeId()).initContext(context, raftNodes)
                                             .thenApply(r -> {
                              ContextConfiguration contextConfiguration = ContextConfiguration.newBuilder()
                                                                                              .setContext(context)
                                                                                              .addAllNodes(clusterNodes)
                                                                                              .build();
                              config.appendEntry(ContextConfiguration.class.getName(),
                                                             contextConfiguration.toByteArray());
                              return r;
                          });
    }

    private NodeInfo addContext(NodeInfo toNodeInfo, String context, String nodeLabel) {
        return NodeInfo.newBuilder(toNodeInfo)
                                     .addContexts(ContextRole.newBuilder().setNodeLabel(nodeLabel).setName(context))
                                     .build();
    }

    @Override
    public void join(NodeInfo nodeInfo) {
        RaftNode config = grpcRaftController.getRaftNode(GrpcRaftController.ADMIN_GROUP);
        List<String> contexts = nodeInfo.getContextsList().stream().map(ContextRole::getName).collect(Collectors
                                                                                                              .toList());
        if (contexts.isEmpty()) {
            contexts = contextController.getContexts().map(io.axoniq.axonserver.enterprise.jpa.Context::getName)
                                                           .collect(Collectors.toList());
        }

        String nodeLabel = generateNodeLabel(nodeInfo.getNodeName());
        Node node = Node.newBuilder().setNodeId(nodeLabel)
                        .setHost(nodeInfo.getInternalHostName())
                        .setPort(nodeInfo.getGrpcInternalPort())
                        .build();
        contexts.forEach(c -> {
            raftGroupServiceFactory.getRaftGroupService(c).addNodeToContext(c, node);

            config.appendEntry(ContextConfiguration.class.getName(),
                                           createContextConfigBuilder(c).addNodes(
                                                   NodeInfoWithLabel.newBuilder().setLabel(nodeLabel).setNode(nodeInfo).build()).build().toByteArray());
        });
    }

    @Override
    public void init(List<String> contexts) {
        String adminLabel = generateNodeLabel(messagingPlatformConfiguration.getName());
        RaftGroup configGroup = grpcRaftController.initRaftGroup(GrpcRaftController.ADMIN_GROUP, adminLabel);
        RaftNode leader = grpcRaftController.waitForLeader(configGroup);
        Node me = Node.newBuilder().setNodeId(adminLabel)
                      .setHost(messagingPlatformConfiguration.getFullyQualifiedInternalHostname())
                      .setPort(messagingPlatformConfiguration.getInternalPort())
                      .build();

        leader.addNode(me);
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
                                                                        .setContext(GrpcRaftController.ADMIN_GROUP)
                                                                        .addNodes(NodeInfoWithLabel.newBuilder().setNode(nodeInfo).setLabel(adminLabel))
                                                                        .build();
        leader.appendEntry(ContextConfiguration.class.getName(), contextConfiguration.toByteArray());

        contexts.forEach(c -> {
            String contextLabel = generateNodeLabel(messagingPlatformConfiguration.getName());
            RaftGroup group = grpcRaftController.initRaftGroup(c, contextLabel);
            RaftNode groupLeader = grpcRaftController.waitForLeader(group);
            Node contextMe = Node.newBuilder().setNodeId(contextLabel)
                          .setHost(messagingPlatformConfiguration.getFullyQualifiedInternalHostname())
                          .setPort(messagingPlatformConfiguration.getInternalPort())
                          .build();

            groupLeader.addNode(contextMe);
            ContextConfiguration groupConfiguration = ContextConfiguration.newBuilder()
                                                                          .setContext(c)
                                                                          .addNodes(NodeInfoWithLabel.newBuilder().setNode(nodeInfo).setLabel(contextLabel))
                                                                          .build();
            leader.appendEntry(ContextConfiguration.class.getName(), groupConfiguration.toByteArray());
        });
    }

    @Override
    public CompletableFuture<Void> updateApplication(Application application) {
        RaftNode config = grpcRaftController.getRaftNode(GrpcRaftController.ADMIN_GROUP);
        CompletableFuture<Void> result = new CompletableFuture<>();
        config.appendEntry(Application.class.getName(), application.toByteArray()).whenComplete(
                (done, throwable) -> {
                    if (throwable != null) {
                        logger.warn("_admin: Failed to add application", throwable);
                        result.completeExceptionally(throwable);
                    } else {
                        result.complete(null);
                        contextController.getContexts()
                                                            .filter(c -> !GrpcRaftController.ADMIN_GROUP.equals(c.getName()))
                                                            .forEach(c -> {
                                             ApplicationContextRole acr = getRolesPerContext(application,
                                                                                             c.getName());
                                             Application.Builder builder = Application.newBuilder(application)
                                                                                      .clearRolesPerContext();
                                             if (acr != null) {
                                                 builder.addRolesPerContext(acr);
                                             }
                                                                raftGroupServiceFactory.getRaftGroupService(c.getName())
                                                                                  .updateApplication(c.getName(), builder.build());
                                         });
                    }
                }
        );
        return result;
    }

    @Override
    public CompletableFuture<Void> updateUser(User request) {
        RaftNode config = grpcRaftController.getRaftNode(GrpcRaftController.ADMIN_GROUP);
        CompletableFuture<Void> result = new CompletableFuture<>();
        config.appendEntry(User.class.getName(), request.toByteArray()).whenComplete(
                (done, throwable) -> {
                    if (throwable != null) {
                        logger.warn("_admin: Failed to add user", throwable);
                        result.completeExceptionally(throwable);
                    } else {
                        result.complete(null);
                        contextController.getContexts()
                                                            .filter(c -> !GrpcRaftController.ADMIN_GROUP.equals(c.getName()))
                                                            .forEach(c -> {
                                                                raftGroupServiceFactory.getRaftGroupService(c.getName())
                                                                                  .updateUser(c.getName(), request);
                                         });
                    }
                }
        );
        return result;
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
        RaftNode config = grpcRaftController.getRaftNode(GrpcRaftController.ADMIN_GROUP);
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
                                                                  .filter(c -> !GrpcRaftController.ADMIN_GROUP.equals(c.getName()))
                                                                  .forEach(c -> {
                                                                      raftGroupServiceFactory.getRaftGroupService(c.getName())
                                                                                                           .updateLoadBalancingStrategy(c.getName(),
                                                                                                                                        loadBalancingStrategy);
                                                                  });
                          }
                      }
              );
        return result;
    }


    @Override
    public CompletableFuture<Void> updateProcessorLoadBalancing(ProcessorLBStrategy processorLBStrategy) {
        RaftNode config = grpcRaftController.getRaftNode(GrpcRaftController.ADMIN_GROUP);
        CompletableFuture<Void> result = new CompletableFuture<>();
        config.appendEntry(ProcessorLBStrategy.class.getName(), processorLBStrategy.toByteArray())
              .whenComplete(
                      (done, throwable) -> {
                          if (throwable != null) {
                              logger.warn("_admin: Failed to set processor load balancing strategies", throwable);
                              result.completeExceptionally(throwable);
                          } else {
                              result.complete(null);
                              raftGroupServiceFactory.getRaftGroupService(processorLBStrategy.getContext())
                                                                   .updateProcessorLoadBalancing(processorLBStrategy.getContext(),
                                                                                                 processorLBStrategy);
                          }
                      }
              );
        return result;
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
    public  CompletableFuture<Void> deleteUser(User request) {
        RaftNode config = grpcRaftController.getRaftNode(GrpcRaftController.ADMIN_GROUP);
        CompletableFuture<Void> result = new CompletableFuture<>();
        config.appendEntry(DELETE_USER, request.toByteArray())
              .whenComplete(
                      (done, throwable) -> {
                          if (throwable != null) {
                              logger.warn("_admin: Failed to delete user", throwable);
                              result.completeExceptionally(throwable);
                          } else {
                              result.complete(null);
                              contextController.getContexts()
                                               .filter(c -> !GrpcRaftController.ADMIN_GROUP.equals(c.getName()))
                                               .forEach(c -> {
                                                   raftGroupServiceFactory.getRaftGroupService(c.getName())
                                                                          .deleteUser(c.getName(), request);
                                               });
                          }
                      }
              );
        return result;
    }

    @Override
    public  CompletableFuture<Void> deleteApplication(Application request) {
        RaftNode config = grpcRaftController.getRaftNode(GrpcRaftController.ADMIN_GROUP);
        CompletableFuture<Void> result = new CompletableFuture<>();
        config.appendEntry(DELETE_APPLICATION, request.toByteArray())
              .whenComplete(
                      (done, throwable) -> {
                          if (throwable != null) {
                              logger.warn("_admin: Failed to delete application", throwable);
                              result.completeExceptionally(throwable);
                          } else {
                              result.complete(null);
                              contextController.getContexts()
                                               .filter(c -> !GrpcRaftController.ADMIN_GROUP.equals(c.getName()))
                                               .forEach(c -> {
                                                   raftGroupServiceFactory.getRaftGroupService(c.getName())
                                                                          .deleteApplication(c.getName(), request);
                                               });
                          }
                      }
              );
        return result;
    }

    @Override
    public CompletableFuture<Void> deleteLoadBalancingStrategy(LoadBalanceStrategy loadBalancingStrategy) {
        RaftNode config = grpcRaftController.getRaftNode(GrpcRaftController.ADMIN_GROUP);
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
                                               .filter(c -> !GrpcRaftController.ADMIN_GROUP.equals(c.getName()))
                                               .forEach(c -> {
                                                   try {
                                                       raftGroupServiceFactory.getRaftGroupService(c.getName())
                                                                              .deleteLoadBalancingStrategy(c.getName(),
                                                                                                           loadBalancingStrategy);
                                                   } catch (Exception ex) {
                                                       logger.warn("{}: Failed to delete load balancing strategy {}", c, loadBalancingStrategy.getName());
                                                   }
                                               });
                          }
                      }
              );
        return result;
    }
}
