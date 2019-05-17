package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.cluster.jpa.JpaRaftGroupNode;
import io.axoniq.axonserver.config.FeatureChecker;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.ContextEvents;
import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import io.axoniq.axonserver.enterprise.cluster.internal.RemoteConnection;
import io.axoniq.axonserver.enterprise.cluster.internal.StubFactory;
import io.axoniq.axonserver.enterprise.config.ClusterConfiguration;
import io.axoniq.axonserver.enterprise.config.FlowControl;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.enterprise.jpa.Context;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.ChannelCloser;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.internal.DeleteNode;
import io.axoniq.axonserver.grpc.internal.NodeInfo;
import io.axoniq.axonserver.licensing.Feature;
import io.axoniq.axonserver.message.ClientIdentification;
import io.axoniq.axonserver.message.command.CommandDispatcher;
import io.axoniq.axonserver.message.query.QueryDispatcher;
import io.axoniq.axonserver.util.StringUtils;
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
 * @author Marc Gathier
 */
@Controller("ClusterController")
public class ClusterController implements SmartLifecycle {

    private final Logger logger = LoggerFactory.getLogger(ClusterController.class);
    private final MessagingPlatformConfiguration messagingPlatformConfiguration;
    private final ClusterConfiguration clusterConfiguration;
    private final EntityManager entityManager;
    private final StubFactory stubFactory;
    private final NodeSelectionStrategy nodeSelectionStrategy;
    private final RaftGroupRepositoryManager raftGroupRepositoryManager;
    private final QueryDispatcher queryDispatcher;
    private final CommandDispatcher commandDispatcher;
    private final ApplicationEventPublisher applicationEventPublisher;
    private final FeatureChecker limits;
    private final ScheduledExecutorService reconnectExecutor = Executors.newSingleThreadScheduledExecutor();
    private final List<Consumer<ClusterEvent>> nodeListeners = new CopyOnWriteArrayList<>();
    private final ConcurrentMap<String, RemoteConnection> remoteConnections = new ConcurrentHashMap<>();
    private final ConcurrentMap<String,ClusterNode> nodeMap = new ConcurrentHashMap<>();
    private final ChannelCloser channelCloser;
    private volatile boolean running;

    public ClusterController(MessagingPlatformConfiguration messagingPlatformConfiguration,
                             ClusterConfiguration clusterConfiguration,
                             EntityManager entityManager,
                             StubFactory stubFactory,
                             NodeSelectionStrategy nodeSelectionStrategy,
                             RaftGroupRepositoryManager raftGroupRepositoryManager,
                             QueryDispatcher queryDispatcher,
                             CommandDispatcher commandDispatcher,
                             ApplicationEventPublisher applicationEventPublisher,
                             FeatureChecker limits,
                             ChannelCloser channelCloser) {
        this.messagingPlatformConfiguration = messagingPlatformConfiguration;
        this.clusterConfiguration = clusterConfiguration;
        this.entityManager = entityManager;
        this.stubFactory = stubFactory;
        this.nodeSelectionStrategy = nodeSelectionStrategy;
        this.raftGroupRepositoryManager = raftGroupRepositoryManager;
        this.queryDispatcher = queryDispatcher;
        this.commandDispatcher = commandDispatcher;
        this.applicationEventPublisher = applicationEventPublisher;
        this.limits = limits;
        this.channelCloser = channelCloser;
    }


    @EventListener
    public void on(ContextEvents.ContextUpdated contextUpdated) {
        nodeMap.clear();
    }


    @Transactional
    public void deleteNode(String name) {
        logger.info("Delete node: {}", name);
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

        }
        applicationEventPublisher.publishEvent(new ClusterEvents.AxonServerNodeDeleted(name));
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

            reconnectExecutor.scheduleWithFixedDelay(this::checkConnections,
                                                     clusterConfiguration.getConnectionCheckDelay(),
                                                     clusterConfiguration.getConnectionCheckInterval(),
                                                     TimeUnit.MILLISECONDS);
        }
        running = true;
    }

    private void checkCurrentNodeSaved() {
        ClusterNode existingClusterNode = entityManager.find(ClusterNode.class,
                                                             messagingPlatformConfiguration.getName());
        if (existingClusterNode == null) {

            ClusterNode clusterNode = new ClusterNode(messagingPlatformConfiguration.getName(),
                                                      messagingPlatformConfiguration.getFullyQualifiedHostname(),
                                                      messagingPlatformConfiguration
                                                              .getFullyQualifiedInternalHostname(),
                                                      messagingPlatformConfiguration.getPort(),
                                                      messagingPlatformConfiguration.getInternalPort(),
                                                      messagingPlatformConfiguration.getHttpPort());
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
            if( ! remoteConnections.containsKey(clusterNode.getName())) {
                RemoteConnection remoteConnection = new RemoteConnection(this, clusterNode,
                                                                         stubFactory,
                                                                         queryDispatcher,
                                                                         commandDispatcher,
                                                                         channelCloser);
                remoteConnections.put(clusterNode.getName(), remoteConnection);

                if (connect) {
                    remoteConnection.init();
                }
            }
        }
    }

    public Collection<RemoteConnection> getRemoteConnections() {
        return remoteConnections.values();
    }

    @Transactional
    public boolean connect(NodeInfo nodeInfo, boolean admin) {
        String nodeName = nodeInfo.getNodeName();
        ClusterNode node = getNode(nodeName);
        if (node == null) {
            if (!admin && !isAnyContext(nodeName)) {
                // received connection from unknown node, and this node is not in any context we know of and not an admin node
                // -> refuse the connection
                return false;
            }
            node = addConnection(nodeInfo);
            RemoteConnection remoteConnection = remoteConnections.remove(nodeName);
            if (remoteConnection != null) {
                remoteConnection.close();
            }
        }


        if (!remoteConnections.containsKey(nodeName)) {
            startRemoteConnection(node, false);
            for (Consumer<ClusterEvent> nodeListener : nodeListeners) {
                nodeListener.accept(new ClusterEvent(ClusterEvent.EventType.NODE_ADDED, node));
            }
        }

        return true;
    }

    private boolean isAnyContext(String nodeName) {
        Set<JpaRaftGroupNode> groups = raftGroupRepositoryManager.findByNodeName(nodeName);
        logger.warn("Checking if node {} is member of any known context, found {}", nodeName, groups.size());
        return ! groups.isEmpty();
    }

    @Transactional
    public synchronized ClusterNode addConnection(NodeInfo nodeInfo) {
        checkLimit(nodeInfo.getNodeName());
        if (nodeInfo.getNodeName().equals(messagingPlatformConfiguration.getName())) {
            logger.warn("Trying to join with current node name: {}", nodeInfo.getNodeName());
            return getMe();
        }
        if (nodeInfo.getInternalHostName().equals(messagingPlatformConfiguration.getInternalHostname())
                && nodeInfo.getGrpcInternalPort() == messagingPlatformConfiguration.getInternalPort()) {
            throw new MessagingPlatformException(ErrorCode.SAME_NODE_NAME, "Cannot join cluster with same hostname and internal port");
        }
        ClusterNode node = merge(nodeInfo);
        if (!remoteConnections.containsKey(node.getName())) {
            startRemoteConnection(node, false);
            nodeListeners.forEach(listener -> listener
                        .accept(new ClusterEvent(ClusterEvent.EventType.NODE_ADDED, node)));
        }
        return node;
    }


    private void checkLimit(String nodeName) {
        if (remoteConnections.containsKey(nodeName) || messagingPlatformConfiguration.getName().equals(nodeName)) {
            return;
        }
        if (limits.getMaxClusterSize() == remoteConnections.size() + 1) {
            throw new MessagingPlatformException(ErrorCode.MAX_CLUSTER_SIZE_REACHED,
                                                 "Maximum allowed number of nodes reached " + nodeName);
        }
    }

    private ClusterNode merge(NodeInfo nodeInfo) {
        synchronized (entityManager) {
            ClusterNode existing = entityManager.find(ClusterNode.class, nodeInfo.getNodeName());
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
            return existing;
        }
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
        Collection<String> nodesInContext = getNodesInContext(context);
        if (nodesInContext.isEmpty()) {
            throw new MessagingPlatformException(ErrorCode.NO_AXONSERVER_FOR_CONTEXT,
                                                 "No AxonServers found for context: " + context);
        }
        if (clientName == null || clientName.isEmpty()) {
            return getMe();
        }

        List<String> activeNodes = new ArrayList<>();
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
        ClusterNode node = getNode(nodeName);
        if( node != null && ! StringUtils.isEmpty(node.getHostName())) return node;
        return getMe();
    }

    private Collection<String> getNodesInContext(String context) {
        if( getMe().isAdmin() ) {
            Context contextJPA = entityManager.find(Context.class, context);
            if( contextJPA != null) {
                return contextJPA.getNodeNames();
            }
        }
        Set<JpaRaftGroupNode> nodes = raftGroupRepositoryManager
                .findByGroupId(context);
        return nodes.stream().map(JpaRaftGroupNode::getNodeName).collect(Collectors.toSet());

    }

    public Set<String> remoteNodeNames() {
        return remoteConnections.keySet();
    }

    public Stream<ClusterNode> nodes() {
        return entityManager.createNamedQuery("ClusterNode.findAll", ClusterNode.class).getResultList()
                            .stream();
    }

    public boolean canRebalance(String clientName, String componentName, String context) {
        Context context1 = entityManager.find(Context.class, context);
        if (context1 == null || context1.getNodes().size() <= 1) {
            return false;
        }
        List<String> activeNodes = new ArrayList<>();
        Collection<String> nodesInContext = context1.getNodeNames();
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
        deleteNode(name);
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

    public FlowControl getCommandFlowControl() {
        return clusterConfiguration.getCommandFlowControl();
    }

    public FlowControl getQueryFlowControl() {
        return clusterConfiguration.getQueryFlowControl();
    }

    public void publishEvent(Object event) {
        applicationEventPublisher.publishEvent(event);
    }

    public long getConnectionWaitTime() {
        return clusterConfiguration.getConnectionWaitTime();
    }

    public void requestDelete(String node) {
        applicationEventPublisher.publishEvent(DeleteNode.newBuilder().setNodeName(node).build());
    }

    /**
     * Event handler for deleting a node. Deleting a node needs to be executed in a transaction.
     * @param deleteRequested event containing the node name
     */
    @EventListener
    @Transactional
    public void on(DeleteNode deleteRequested) {
        deleteNode(deleteRequested.getNodeName());
    }

    public boolean isAdminNode() {
        return getMe().isAdmin();
    }

    /**
     * sets up a connection to a node received through a newconfiguration log entry.
     * Node information does not contain full information of the remote node (only name, internal hostname and internal port)
     * so if it is an unknown host it will set-up a temporary connection that is updated once the remote node connects.
     *
     * @param node the node to connect to
     */
    public void connect(Node node) {
        ClusterNode existingClusterNode = entityManager.find(ClusterNode.class, node.getNodeName());

        if (existingClusterNode == null) {
            startRemoteConnection(new ClusterNode(node), true);
        }
    }
}
