package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.access.modelversion.ModelVersionController;
import io.axoniq.axonserver.config.ClusterConfiguration;
import io.axoniq.axonserver.config.FlowControl;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import io.axoniq.axonserver.enterprise.cluster.events.ContextEvents;
import io.axoniq.axonserver.enterprise.cluster.internal.RemoteConnection;
import io.axoniq.axonserver.enterprise.cluster.internal.StubFactory;
import io.axoniq.axonserver.enterprise.context.NodeRoles;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.enterprise.jpa.Context;
import io.axoniq.axonserver.enterprise.jpa.ContextClusterNode;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.features.Feature;
import io.axoniq.axonserver.features.FeatureChecker;
import io.axoniq.axonserver.grpc.internal.ConnectorCommand;
import io.axoniq.axonserver.grpc.internal.ContextRole;
import io.axoniq.axonserver.grpc.internal.NodeInfo;
import io.axoniq.axonserver.message.ClientIdentification;
import io.axoniq.axonserver.message.command.CommandDispatcher;
import io.axoniq.axonserver.message.query.QueryDispatcher;
import io.axoniq.axonserver.rest.ClusterRestController;
import io.axoniq.axonserver.topology.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Controller;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.persistence.EntityManager;

/**
 * Author: marc
 */
@Controller("ClusterController")
public class ClusterController implements SmartLifecycle {

    private final Logger logger = LoggerFactory.getLogger(ClusterController.class);
    private final MessagingPlatformConfiguration messagingPlatformConfiguration;
    private final EntityManager entityManager;
    private final StubFactory stubFactory;
    private final NodeSelectionStrategy nodeSelectionStrategy;
    private final QueryDispatcher queryDispatcher;
    private final CommandDispatcher commandDispatcher;
    private final ModelVersionController modelVersionController;
    private final ApplicationEventPublisher applicationEventPublisher;
    private final FeatureChecker limits;
    private final ScheduledExecutorService reconnectExecutor = Executors.newSingleThreadScheduledExecutor();
    private final List<Consumer<ClusterEvent>> nodeListeners = new CopyOnWriteArrayList<>();
    private final ConcurrentMap<String, RemoteConnection> remoteConnections = new ConcurrentHashMap<>();
    private final ConcurrentMap<String,ClusterNode> nodeMap = new ConcurrentHashMap<>();
    private volatile boolean running;

    public ClusterController(MessagingPlatformConfiguration messagingPlatformConfiguration,
                             EntityManager entityManager,
                             StubFactory stubFactory,
                             NodeSelectionStrategy nodeSelectionStrategy,
                             QueryDispatcher queryDispatcher,
                             CommandDispatcher commandDispatcher,
                             ModelVersionController modelVersionController,
                             ApplicationEventPublisher applicationEventPublisher,
                             FeatureChecker limits
    ) {
        this.messagingPlatformConfiguration = messagingPlatformConfiguration;
        this.entityManager = entityManager;
        this.stubFactory = stubFactory;
        this.nodeSelectionStrategy = nodeSelectionStrategy;
        this.queryDispatcher = queryDispatcher;
        this.commandDispatcher = commandDispatcher;
        this.modelVersionController = modelVersionController;
        this.applicationEventPublisher = applicationEventPublisher;
        this.limits = limits;
    }


    @EventListener
    @Transactional
    public void on(ClusterEvents.AxonServerNodeDeleted nodeDeleted) {
        deleteNode(nodeDeleted.node(), nodeDeleted.getGeneration());
    }

    @Transactional
    public void deleteNode(String name, long generation) {
        logger.warn("Delete node: {}", name);
        synchronized (remoteConnections) {
            if (messagingPlatformConfiguration.getName().equals(name)) {
                remoteConnections.forEach((node, rc) -> rc.close());
                remoteConnections.clear();

                List<ClusterNode> otherNodes = entityManager
                        .createQuery("select c from ClusterNode c where c.name <> :name", ClusterNode.class)
                        .setParameter("name", name)
                        .getResultList();

                otherNodes.forEach(node -> {
                    entityManager.remove(node);
                    nodeMap.remove(node.getName());
                    nodeListeners.forEach(listener -> listener
                            .accept(new ClusterEvent(ClusterEvent.EventType.NODE_DELETED, node)));
                });
            }

            RemoteConnection remoteConnection = remoteConnections.remove(name);
            if (remoteConnection != null) {
                ClusterNode node = entityManager.find(ClusterNode.class, name);
                if (node != null) {
                    entityManager.remove(node);
                    entityManager.flush();
                }
                remoteConnection.close();
                nodeListeners.forEach(listener -> listener
                        .accept(new ClusterEvent(ClusterEvent.EventType.NODE_DELETED,
                                                 remoteConnection.getClusterNode())));
                nodeMap.remove(name);
            }

            modelVersionController.updateModelVersion(ClusterNode.class, generation);
        }
        applicationEventPublisher.publishEvent(new ClusterEvents.ClusterUpdatedNotification());
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    @Override
    public void stop(Runnable runnable) {
        reconnectExecutor.shutdownNow();
        remoteConnections.forEach((k, v) -> v.close());
        runnable.run();
        running = false;
    }

    @Override
    @Transactional
    public void start() {
        checkCurrentNodeSaved();
        if (Feature.CLUSTERING.enabled(limits)) {
            logger.debug("Start cluster controller");

            nodes().forEach(clusterNode -> startRemoteConnection(clusterNode, true));
            ClusterConfiguration clusterConfiguration = messagingPlatformConfiguration.getCluster();

            reconnectExecutor.scheduleWithFixedDelay(this::checkConnections,
                                                     clusterConfiguration.getConnectionCheckDelay(),
                                                     clusterConfiguration.getConnectionCheckInterval(),
                                                     TimeUnit.MILLISECONDS);
        }
        running = true;
    }

    public boolean isClustered() {
        return Feature.CLUSTERING.enabled(limits);
    }

    private void checkCurrentNodeSaved() {
        ClusterNode existingClusterNode = entityManager.find(ClusterNode.class,
                                                             messagingPlatformConfiguration.getName());
        if (existingClusterNode != null) {
            existingClusterNode.setGrpcInternalPort(messagingPlatformConfiguration.getInternalPort());
            existingClusterNode.setGrpcPort(messagingPlatformConfiguration.getPort());
            existingClusterNode.setHostName(messagingPlatformConfiguration.getFullyQualifiedHostname());
            existingClusterNode.setHttpPort(messagingPlatformConfiguration.getHttpPort());
            existingClusterNode.setInternalHostName(messagingPlatformConfiguration.getFullyQualifiedInternalHostname());
        } else {

            List<ClusterNode> clusterNodes = entityManager.createNamedQuery("ClusterNode.findByInternalHostNameAndPort", ClusterNode.class)
                                                          .setParameter("internalHostName",
                                                                        messagingPlatformConfiguration
                                                                                .getFullyQualifiedInternalHostname())
                                                          .setParameter("internalPort",
                                                                        messagingPlatformConfiguration
                                                                                .getInternalPort()).getResultList();

            ClusterNode clusterNode = new ClusterNode(messagingPlatformConfiguration.getName(),
                                                      messagingPlatformConfiguration.getFullyQualifiedHostname(),
                                                      messagingPlatformConfiguration
                                                              .getFullyQualifiedInternalHostname(),
                                                      messagingPlatformConfiguration.getPort(),
                                                      messagingPlatformConfiguration.getInternalPort(),
                                                      messagingPlatformConfiguration.getHttpPort());
            if (!clusterNodes.isEmpty()) {
                Set<ContextClusterNode> contextNames = clusterNodes.get(0).getContexts();
                entityManager.remove(clusterNodes.get(0));
                entityManager.flush();


                contextNames.forEach(contextName -> {
                    ContextClusterNode contextClusterNode = new ContextClusterNode(contextName.getContext(),
                                                                                   clusterNode);
                    contextClusterNode.setMessaging(contextName.isMessaging());
                    contextClusterNode.setStorage(contextName.isStorage());
                    clusterNode.addContext(contextClusterNode);
                });
            } else {
                clusterNode.addContext(entityManager.find(Context.class, Topology.DEFAULT_CONTEXT), true, true);
            }
            entityManager.persist(clusterNode);
        }
    }

    Stream<RemoteConnection> activeConnections() {
        return remoteConnections.values()
                                .stream()
                                .filter(RemoteConnection::isConnected);
    }

    private void checkConnections() {
        remoteConnections.values().forEach(RemoteConnection::checkConnection);
    }

    @Override
    public void stop() {
        stop(() -> {
        });
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return 50;
    }

    private void startRemoteConnection(ClusterNode clusterNode, boolean connect) {
        if (clusterNode.getName().equals(messagingPlatformConfiguration.getName())) {
            return;
        }

        synchronized (remoteConnections) {
            RemoteConnection remoteConnection = new RemoteConnection(this, clusterNode,
                                                                     stubFactory,
                                                                     queryDispatcher,
                                                                     commandDispatcher);
            remoteConnections.put(clusterNode.getName(), remoteConnection);

            if (connect) {
                remoteConnection.init();
            }
        }
    }

    public Collection<RemoteConnection> getRemoteConnections() {
        return remoteConnections.values();
    }

    @EventListener
    @Transactional
    public void on(ClusterEvents.AxonServerInstanceConnected axonHubInstanceConnected) {
        synchronized (entityManager) {
            if (axonHubInstanceConnected.getNodesList() != null) {
                axonHubInstanceConnected.getNodesList().forEach(nodeInfo -> addConnection(nodeInfo,
                                                                                          axonHubInstanceConnected
                                                                                                  .getGeneration()));
                if( currentGeneration() < axonHubInstanceConnected.getGeneration()) {
                    setGeneration(axonHubInstanceConnected.getGeneration());
                }
            }
        }
    }

    @Transactional
    public long joinConnection(NodeInfo request) {
        long nextGeneration  = currentGeneration() + 1;
        addConnection(request, nextGeneration);
        setGeneration(nextGeneration);
        return nextGeneration;
    }

    @Transactional
    public boolean addConnection(NodeInfo nodeInfo, long generation) {
        checkLimit(nodeInfo.getNodeName());
        if (nodeInfo.getNodeName().equals(messagingPlatformConfiguration.getName())) {
            throw new MessagingPlatformException(ErrorCode.SAME_NODE_NAME, "Cannot join cluster with same node name");
        }
        if (nodeInfo.getInternalHostName().equals(messagingPlatformConfiguration.getInternalHostname())
                && nodeInfo.getGrpcInternalPort() == messagingPlatformConfiguration.getInternalPort()) {
            throw new MessagingPlatformException(ErrorCode.SAME_NODE_NAME, "Cannot join cluster with same hostname and internal port");
        }
        ClusterNode node = merge(nodeInfo, generation);
        if( node == null) {
            applicationEventPublisher.publishEvent(new ClusterEvents.ClusterUpdatedNotification());
            return false;
        }
        if (!remoteConnections.containsKey(node.getName())) {
            startRemoteConnection(node, false);
            nodeListeners.forEach(listener -> listener
                        .accept(new ClusterEvent(ClusterEvent.EventType.NODE_ADDED, node)));
        }
        return true;
    }


    private void checkLimit(String nodeName) {
        if (remoteConnections.containsKey(nodeName)) {
            return;
        }
        if (limits.getMaxClusterSize() == remoteConnections.size() + 1) {
            throw new MessagingPlatformException(ErrorCode.MAX_CLUSTER_SIZE_REACHED,
                                                 "Maximum allowed number of nodes reached");
        }
    }

    private ClusterNode merge(NodeInfo nodeInfo, long generation) {
        ClusterNode existing = entityManager.find(ClusterNode.class, nodeInfo.getNodeName());
        long currentGeneration = modelVersionController.getModelVersion(ClusterNode.class);
        if (existing == null) {
            existing = findFirstByInternalHostNameAndGrpcInternalPort(nodeInfo.getInternalHostName(),
                                                                      nodeInfo.getGrpcInternalPort());
            if (existing != null) {
                entityManager.remove(existing);
                entityManager.flush();
                RemoteConnection remoteConnection = remoteConnections.remove(existing.getName());
                if (remoteConnection != null) {
                    remoteConnection.close();
                }
            } else {
                if( currentGeneration > generation) {
                    return null;
                }
            }
            existing = ClusterNode.from(nodeInfo);
            entityManager.persist(existing);
        } else {
            existing.setGrpcInternalPort(nodeInfo.getGrpcInternalPort());
            existing.setGrpcPort(nodeInfo.getGrpcPort());
            existing.setHostName(nodeInfo.getHostName());
            existing.setHttpPort(nodeInfo.getHttpPort());
            existing.setInternalHostName(nodeInfo.getInternalHostName());
        }
        if( generation > currentGeneration) {
            for (ContextRole context : nodeInfo.getContextsList()) {
                mergeContext(existing, context.getName(), context.getStorage(), context.getMessaging());
            }
        }
        return existing;
    }

    private void mergeContext(ClusterNode existing, String context, boolean storage, boolean messaging) {
        Context contextObj = entityManager.find(Context.class,  context);
        if (contextObj == null) {
            contextObj = new Context(context);
            entityManager.persist(contextObj);
        }
        existing.addContext(contextObj, storage, messaging);
    }

    private ClusterNode findFirstByInternalHostNameAndGrpcInternalPort(String internalHostName, int grpcInternalPort) {
        List<ClusterNode> clusterNodes = entityManager.createNamedQuery("ClusterNode.findByInternalHostNameAndPort", ClusterNode.class)
                                                      .setParameter("internalHostName",
                                                                    internalHostName)
                                                      .setParameter("internalPort",
                                                                    grpcInternalPort).getResultList();
        if (clusterNodes.isEmpty()) {
            return null;
        }
        return clusterNodes.get(0);
    }

    public ClusterNode getMe() {
        return entityManager.find(ClusterNode.class, messagingPlatformConfiguration.getName());
    }


    public boolean isActive(String nodeName) {
        return nodeName.equals(messagingPlatformConfiguration.getName()) ||
                (remoteConnections.get(nodeName) != null && remoteConnections.get(nodeName)
                                                                                          .isConnected());
    }

    public ClusterNode findNodeForClient(String clientName, String componentName, String context) {
        Context context1 = entityManager.find(Context.class, context);
        if (context1 == null) {
            throw new MessagingPlatformException(ErrorCode.NO_AXONSERVER_FOR_CONTEXT,
                                                 "No AxonHub servers found for context: " + context);
        }
        if (clientName == null || clientName.isEmpty()) {
            return getMe();
        }

        List<String> activeNodes = new ArrayList<>();
        Collection<String> nodesInContext = context1.getMessagingNodeNames();
        if (nodesInContext.contains(messagingPlatformConfiguration.getName())) {
            activeNodes.add(messagingPlatformConfiguration.getName());
        }
        nodesInContext.stream().map(remoteConnections::get).filter(remoteConnection -> remoteConnection != null &&
                remoteConnection.isConnected()).forEach(e -> activeNodes.add(e.getClusterNode().getName()));

        if (activeNodes.isEmpty()) {
            throw new MessagingPlatformException(ErrorCode.NO_AXONSERVER_FOR_CONTEXT,
                                                 "No active Axon servers found for context: " + context);
        }
        String nodeName = nodeSelectionStrategy.selectNode(new ClientIdentification(context,clientName), componentName, activeNodes);
        if (remoteConnections.containsKey(nodeName)) {
            return remoteConnections.get(nodeName).getClusterNode();
        }
        return getMe();
    }


    public Stream<ClusterNode> nodes() {
        return entityManager.createNamedQuery("ClusterNode.findAll", ClusterNode.class).getResultList()
                            .stream();
    }

    public boolean canRebalance(String clientName, String componentName, String context) {
        Context context1 = entityManager.find(Context.class, context);
        if (context1 == null || context1.getMessagingNodes().size() <= 1) {
            return false;
        }
        List<String> activeNodes = new ArrayList<>();
        Collection<String> nodesInContext = context1.getMessagingNodeNames();
        if (nodesInContext.contains(messagingPlatformConfiguration.getName())) {
            activeNodes.add(messagingPlatformConfiguration.getName());
        }
        remoteConnections.entrySet().stream().filter(e -> e.getValue().isConnected()).forEach(e -> activeNodes
                .add(e.getKey()));
        if (activeNodes.size() <= 1) {
            return false;
        }

        return nodeSelectionStrategy.canRebalance(new ClientIdentification(context,clientName), componentName, activeNodes);
    }


    public void addNodeListener(Consumer<ClusterEvent> nodeListener) {
        nodeListeners.add(nodeListener);
    }

    @Transactional
    public void sendDeleteNode(String name) {
        long generation = modelVersionController.incrementModelVersion(ClusterNode.class);
        remoteConnections.values().forEach(remoteConnection -> remoteConnection.sendDelete(name, generation));
        deleteNode(name, generation);
    }

    public void publish(ConnectorCommand connectorCommand) {
        activeConnections().forEach(remoteConnection -> remoteConnection.publish(connectorCommand));
    }

    public Set<String> getMyContextsNames() {
        return getMe().getContextNames();
    }

    public Set<ContextClusterNode> getMyContexts() {
        return getMe().getContexts();
    }

    public Set<String> getMyStorageContexts() {
        return getMe().getStorageContexts().stream().map(Context::getName).collect(Collectors.toSet());
    }

    public Set<String> getMyMessagingContexts() {
        return getMe().getMessagingContexts().stream().map(Context::getName).collect(Collectors.toSet());
    }

    public void closeConnection(String nodeName) {
        if (remoteConnections.containsKey(nodeName)) {
            remoteConnections.get(nodeName).close();
        }
    }

    public String getName() {
        return messagingPlatformConfiguration.getName();
    }

    public ClusterNode getNode(String name) {
        return nodeMap.computeIfAbsent(name, n -> entityManager.find(ClusterNode.class, n));
    }

    @Transactional
    public void setMyContexts(List<ClusterRestController.ContextRoleJSON> contexts) {
        ClusterNode clusterNode = getMe();
        Set<String> oldStorageContexts = getMyStorageContexts();
        Set<String> oldContexts = getMyContextsNames();
        for(ClusterRestController.ContextRoleJSON contextRoleJSON : contexts) {
            mergeContext(clusterNode, contextRoleJSON.getName(), contextRoleJSON.isStorage(), contextRoleJSON.isMessaging());
            if( contextRoleJSON.isStorage() && ! oldStorageContexts.remove(contextRoleJSON.getName()) ) {
                    applicationEventPublisher.publishEvent(new ContextEvents.NodeRolesUpdated(contextRoleJSON.getName(),
                                                                                              new NodeRoles(getName(),contextRoleJSON.isMessaging(), contextRoleJSON.isStorage()), false));
            }
            oldContexts.remove(contextRoleJSON.getName());
        }

        logger.debug("Leaving storage contexts: {}", oldStorageContexts);
        oldStorageContexts.forEach(context -> applicationEventPublisher.publishEvent(
                new ClusterEvents.MasterStepDown(context, true)));


        oldContexts.forEach(clusterNode::removeContext);
        entityManager.flush();
    }

    @EventListener
    public void on(ContextEvents.BaseContextEvent contextEvent) {
        nodeMap.clear();
    }

    public void publishTo(String nodeName, ConnectorCommand connectorCommand) {
        if( remoteConnections.containsKey(nodeName))
            remoteConnections.get(nodeName).publish(connectorCommand);
    }

    public boolean disconnectedNodes() {
        return remoteConnections.values().stream().anyMatch(r -> !r.isConnected());
    }

    public void setGeneration(long generation) {
        modelVersionController.updateModelVersion(ClusterNode.class, generation);
    }

    public long currentGeneration() {
        return modelVersionController.getModelVersion(ClusterNode.class);
    }

    public FlowControl getCommandFlowControl() {
        return messagingPlatformConfiguration.getCommandFlowControl();
    }

    public FlowControl getQueryFlowControl() {
        return messagingPlatformConfiguration.getQueryFlowControl();
    }

    public void publishEvent(Object event) {
        applicationEventPublisher.publishEvent(event);
    }

    public long getConnectionWaitTime() {
        return messagingPlatformConfiguration.getCluster().getConnectionWaitTime();
    }

}
