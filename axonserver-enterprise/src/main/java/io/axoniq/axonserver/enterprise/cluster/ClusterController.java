package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.ClusterTagsCache;
import io.axoniq.axonserver.config.FeatureChecker;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.ContextEvents;
import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import io.axoniq.axonserver.enterprise.cluster.internal.RemoteConnection;
import io.axoniq.axonserver.enterprise.cluster.internal.StubFactory;
import io.axoniq.axonserver.enterprise.config.ClusterConfiguration;
import io.axoniq.axonserver.enterprise.config.FlowControl;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.ChannelCloser;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.internal.DeleteNode;
import io.axoniq.axonserver.grpc.internal.NodeInfo;
import io.axoniq.axonserver.licensing.Feature;
import io.axoniq.axonserver.message.command.CommandDispatcher;
import io.axoniq.axonserver.message.query.QueryDispatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Controller;
import org.springframework.transaction.annotation.Transactional;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

/**
 * @author Marc Gathier
 */
@Controller("ClusterController")
public class ClusterController implements SmartLifecycle, ApplicationContextAware {

    private final Logger logger = LoggerFactory.getLogger(ClusterController.class);
    private final MessagingPlatformConfiguration messagingPlatformConfiguration;
    private final ClusterConfiguration clusterConfiguration;
    private final ClusterNodeRepository clusterNodeRepository;
    private final ClusterTagsCache clusterTagsCache;
    private final StubFactory stubFactory;
    private final QueryDispatcher queryDispatcher;
    private final CommandDispatcher commandDispatcher;
    private final ApplicationEventPublisher applicationEventPublisher;
    private final FeatureChecker limits;
    private final ChannelCloser channelCloser;
    private final ScheduledExecutorService reconnectExecutor = Executors.newSingleThreadScheduledExecutor();
    private final List<Consumer<ClusterEvent>> nodeListeners = new CopyOnWriteArrayList<>();
    private final ConcurrentMap<String, RemoteConnection> remoteConnections = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ClusterNode> nodeMap = new ConcurrentHashMap<>();
    private volatile boolean running;
    private ApplicationContext applicationContext;

    public ClusterController(MessagingPlatformConfiguration messagingPlatformConfiguration,
                             ClusterConfiguration clusterConfiguration,
                             ClusterNodeRepository clusterNodeRepository,
                             ClusterTagsCache clusterTagsCache,
                             StubFactory stubFactory,
                             QueryDispatcher queryDispatcher,
                             CommandDispatcher commandDispatcher,
                             @Qualifier("localEventPublisher") ApplicationEventPublisher applicationEventPublisher,
                             FeatureChecker limits,
                             ChannelCloser channelCloser) {
        this.messagingPlatformConfiguration = messagingPlatformConfiguration;
        this.clusterConfiguration = clusterConfiguration;
        this.clusterNodeRepository = clusterNodeRepository;
        this.clusterTagsCache = clusterTagsCache;
        this.stubFactory = stubFactory;
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

                clusterNodeRepository.deleteAllByNameNot(name);
            }

            RemoteConnection remoteConnection = remoteConnections.remove(name);
            if (remoteConnection != null) {
                clusterNodeRepository.findById(name).ifPresent(c -> clusterNodeRepository.deleteById(name));
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
        reconnectExecutor.shutdown();
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
        Optional<ClusterNode> optionalNode = clusterNodeRepository.findById(messagingPlatformConfiguration.getName());
        if (!optionalNode.isPresent()) {
            if (!clusterNodeRepository.findAll().isEmpty()) {
                logger.error("Current node name has changed, new name {}. Start AxonServer with recovery file.",
                             messagingPlatformConfiguration.getName());
                SpringApplication.exit(applicationContext, () -> {
                    System.exit(1);
                    return 1;
                });
            }

            ClusterNode newNode = new ClusterNode(messagingPlatformConfiguration.getName(),
                                                  messagingPlatformConfiguration
                                                          .getFullyQualifiedHostname(),
                                                  messagingPlatformConfiguration
                                                          .getFullyQualifiedInternalHostname(),
                                                  messagingPlatformConfiguration.getPort(),
                                                  messagingPlatformConfiguration
                                                          .getInternalPort(),
                                                  messagingPlatformConfiguration
                                                          .getHttpPort());
            clusterNodeRepository.save(newNode);
        } else {
            ClusterNode node = optionalNode.get();
            if (!node.getInternalHostName().equals(messagingPlatformConfiguration
                                                                 .getFullyQualifiedInternalHostname()) ||
                    !node.getGrpcInternalPort().equals(messagingPlatformConfiguration.getInternalPort())) {
                logger.error(
                        "Current node's internal hostname/port ({}:{}) has changed,  new values {}:{}. Start AxonServer with recovery file.",
                        node.getInternalHostName(), node.getGrpcInternalPort(),
                        messagingPlatformConfiguration.getFullyQualifiedHostname(), messagingPlatformConfiguration.getInternalPort());
                SpringApplication.exit(applicationContext, () -> {
                    System.exit(1);
                    return 1;
                });
            }
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
            if (!remoteConnections.containsKey(clusterNode.getName())) {
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

    /**
     * Received connection from another Axon Server node. Update configuration if this node is not known.
     * Needs to be synchronized as it can be called in parallel with same node information, which would
     * cause a unique key violation.
     * Only accepts connections if the other node is member of a context of this node or it is an admin node.
     *
     * @param nodeInfo the node information of the node connecting to this node
     */
    @Transactional
    public synchronized void handleRemoteConnection(NodeInfo nodeInfo) {
        String nodeName = nodeInfo.getNodeName();
        ClusterNode node = getNode(nodeName);
        if (node == null) {
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

        applicationEventPublisher.publishEvent(new ClusterEvents.AxonServerNodeConnected(nodeInfo));
    }

    @Transactional
    public synchronized ClusterNode addConnection(NodeInfo nodeInfo) {
        checkLimit(nodeInfo.getNodeName());
        if (nodeInfo.getNodeName().equals(messagingPlatformConfiguration.getName())) {
            logger.info("Trying to join with current node name: {}", nodeInfo.getNodeName());
            return getMe();
        }
        if (nodeInfo.getInternalHostName().equals(messagingPlatformConfiguration.getInternalHostname())
                && nodeInfo.getGrpcInternalPort() == messagingPlatformConfiguration.getInternalPort()) {
            throw new MessagingPlatformException(ErrorCode.SAME_NODE_NAME,
                                                 "Cannot join cluster with same hostname and internal port");
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
        synchronized (clusterNodeRepository) {
            ClusterNode clusterNode;
            Optional<ClusterNode> existing = clusterNodeRepository.findById(nodeInfo.getNodeName());
            if (!existing.isPresent()) {
                existing = clusterNodeRepository
                        .findFirstByInternalHostNameAndGrpcInternalPort(nodeInfo.getInternalHostName(),
                                                                        nodeInfo.getGrpcInternalPort());
                if (existing.isPresent()) {
                    clusterNodeRepository.delete(existing.get());

                    RemoteConnection remoteConnection = remoteConnections.remove(existing.get().getName());
                    if (remoteConnection != null) {
                        remoteConnection.close();
                    }
                }
                clusterNode = ClusterNode.from(nodeInfo);
            } else {
                clusterNode = existing.get();
                clusterNode.setGrpcInternalPort(nodeInfo.getGrpcInternalPort());
                clusterNode.setGrpcPort(nodeInfo.getGrpcPort());
                clusterNode.setHostName(nodeInfo.getHostName());
                clusterNode.setHttpPort(nodeInfo.getHttpPort());
                clusterNode.setInternalHostName(nodeInfo.getInternalHostName());
            }
            clusterNodeRepository.save(clusterNode);
            return clusterNode;
        }
    }

    public ClusterNode getMe() {
        return clusterNodeRepository.findById(messagingPlatformConfiguration.getName())
                                    .map(this::setTags)
                                    .orElseThrow(() -> new MessagingPlatformException(ErrorCode.NO_SUCH_NODE,
                                                                                      "Current node not found"));
    }


    public boolean isActive(String nodeName) {
        return nodeName.equals(messagingPlatformConfiguration.getName()) ||
                (remoteConnections.get(nodeName) != null && remoteConnections.get(nodeName)
                                                                             .isConnected());
    }


    public Set<String> remoteNodeNames() {
        return remoteConnections.keySet();
    }

    public Stream<ClusterNode> nodes() {
        return clusterNodeRepository
                .findAll()
                .stream()
                .peek(this::setTags);
    }

    public Stream<ClusterNode> activeNodes() {
        return nodes().filter(n -> isActive(n.getName()));
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
        ClusterNode clusterNode = nodeMap.computeIfAbsent(name, id -> clusterNodeRepository.findById(id).orElse(null));
        return clusterNode == null ? null : setTags(clusterNode);
    }

    private ClusterNode setTags(ClusterNode clusterNode) {
        clusterNode.setTags(clusterTagsCache.getClusterTags()
                                            .getOrDefault(clusterNode.getName(), Collections.emptyMap()));
        return clusterNode;
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
     *
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
     * Node information does not contain full information of the remote node (only name, internal hostname and internal
     * port)
     * so if it is an unknown host it will set-up a temporary connection that is updated once the remote node connects.
     *
     * If the node is already known, no action is performed.
     *
     * @param node the node to connect to
     */
    public void connect(Node node) {
        if (!clusterNodeRepository.findById(node.getNodeName()).isPresent()) {
            startRemoteConnection(new ClusterNode(node), true);
        }
    }

    @Override
    public void setApplicationContext(@Nonnull ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }
}
