package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.cluster.LogEntryConsumer;
import io.axoniq.axonserver.cluster.NewConfigurationConsumer;
import io.axoniq.axonserver.cluster.RaftGroup;
import io.axoniq.axonserver.cluster.RaftNode;
import io.axoniq.axonserver.cluster.StateChanged;
import io.axoniq.axonserver.cluster.grpc.RaftGroupManager;
import io.axoniq.axonserver.cluster.jpa.JpaRaftGroupNode;
import io.axoniq.axonserver.cluster.jpa.JpaRaftGroupNodeRepository;
import io.axoniq.axonserver.cluster.jpa.JpaRaftStateRepository;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.ContextEvents;
import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import io.axoniq.axonserver.enterprise.cluster.internal.ReplicationServerStarted;
import io.axoniq.axonserver.enterprise.config.RaftProperties;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.cluster.Role;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import io.axoniq.axonserver.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Controller;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static io.axoniq.axonserver.RaftAdminGroup.getAdmin;
import static io.axoniq.axonserver.RaftAdminGroup.isAdmin;
import static java.util.Collections.singletonList;

/**
 * @author Marc Gathier
 */
@Controller
public class GrpcRaftController implements SmartLifecycle, RaftGroupManager {

    private static final boolean NO_EVENT_STORE = false;
    private final Logger logger = LoggerFactory.getLogger(GrpcRaftController.class);
    private final JpaRaftStateRepository raftStateRepository;
    private final MessagingPlatformConfiguration messagingPlatformConfiguration;
    private final Map<String,RaftGroup> raftGroupMap = new ConcurrentHashMap<>();
    private boolean running;
    private volatile boolean replicationServerStarted;
    private final RaftGroupRepositoryManager raftGroupNodeRepository;
    private final RaftProperties raftProperties;
    private final ApplicationEventPublisher eventPublisher;
    private final AxonServerGrpcRaftClientFactory grpcRaftClientFactory;
    private final ApplicationContext applicationContext;
    private final PlatformTransactionManager platformTransactionManager;
    private final JpaRaftGroupNodeRepository nodeRepository;
    private final SnapshotDataProviders snapshotDataProviders;
    private volatile LocalEventStore localEventStore;

    public GrpcRaftController(JpaRaftStateRepository raftStateRepository,
                              MessagingPlatformConfiguration messagingPlatformConfiguration,
                              RaftGroupRepositoryManager raftGroupNodeRepository,
                              RaftProperties raftProperties,
                              ApplicationEventPublisher eventPublisher,
                              JpaRaftGroupNodeRepository nodeRepository,
                              SnapshotDataProviders snapshotDataProviders,
                              AxonServerGrpcRaftClientFactory grpcRaftClientFactory,
                              ApplicationContext applicationContext,
                              PlatformTransactionManager platformTransactionManager) {
        this.raftStateRepository = raftStateRepository;
        this.messagingPlatformConfiguration = messagingPlatformConfiguration;
        this.raftGroupNodeRepository = raftGroupNodeRepository;
        this.raftProperties = raftProperties;
        this.eventPublisher = eventPublisher;
        this.nodeRepository = nodeRepository;
        this.snapshotDataProviders = snapshotDataProviders;
        this.grpcRaftClientFactory = grpcRaftClientFactory;
        this.applicationContext = applicationContext;
        this.platformTransactionManager = platformTransactionManager;
    }


    public void start() {
        localEventStore = applicationContext.getBean(LocalEventStore.class);
        Set<JpaRaftGroupNode> groups = raftGroupNodeRepository.getMyContexts();
        groups.forEach(context -> {
            try {
                createRaftGroup(context.getGroupId(), context.getNodeId(), NO_EVENT_STORE, context.getRole());
            } catch (Exception ex) {
                logger.warn("{}: Failed to initialize context", context.getGroupId(), ex);
            }
        });
        running = true;
    }

    @Override
    public void stop() {
        stop(()->{});

    }

    @Override
    public boolean isRunning() {
        return running;
    }


    /**
     * Creates a new raft group (when a context is created) with the current node as a primary member.
     * Initializes the event store for the context (if the group is not the _admin group)
     *
     * @param groupId   the name of the raft group
     * @param nodeLabel the unique id of the node
     * @param nodeName  the name of the node
     * @return the created raft group
     */
    public RaftGroup initRaftGroup(String groupId, String nodeLabel, String nodeName) {
        Node node = Node.newBuilder()
                        .setNodeId(nodeLabel)
                        .setHost(messagingPlatformConfiguration.getFullyQualifiedInternalHostname())
                        .setPort(messagingPlatformConfiguration.getInternalPort())
                        .setNodeName(nodeName)
                        .setRole(Role.PRIMARY)
                        .build();
        RaftGroup raftGroup = createRaftGroup(groupId, nodeLabel, !isAdmin(groupId), Role.PRIMARY);
        raftGroup.raftConfiguration().update(singletonList(node));
        if( replicationServerStarted) {
            raftGroup.connect();
        }
        return raftGroup;
    }

    private RaftGroup createRaftGroup(String groupId, String localNodeId, boolean initializeEventStore, Role role) {
        synchronized (raftGroupMap) {
            RaftGroup existingRaftGroup = raftGroupMap.get(groupId);
            if( existingRaftGroup != null) return existingRaftGroup;

            NewConfigurationConsumer newConfigurationConsumer = applicationContext
                    .getBean(NewConfigurationConsumer.class);

            RaftGroup raftGroup = new GrpcRaftGroup(localNodeId,
                                                    groupId,
                                                    raftStateRepository,
                                                    nodeRepository,
                                                    raftProperties,
                                                    snapshotDataProviders,
                                                    localEventStore,
                                                    grpcRaftClientFactory,
                                                    messagingPlatformConfiguration,
                                                    newConfigurationConsumer,
                                                    platformTransactionManager);

            if (initializeEventStore) {
                eventPublisher.publishEvent(new ContextEvents.ContextCreated(groupId));
            }
            applicationContext.getBeansOfType(LogEntryConsumer.class)
                              .forEach((name, bean) -> raftGroup.localNode()
                                                                .registerEntryConsumer(bean));
            raftGroup.localNode().registerStateChangeListener(stateChanged -> stateChanged(raftGroup.localNode(),
                                                                                           stateChanged));

            raftGroupMap.put(groupId, raftGroup);
            if (replicationServerStarted) {
                raftGroup.localNode().start(role);
            }
            return raftGroup;
        }
    }

    private void stateChanged(RaftNode node, StateChanged stateChanged) {
        if( stateChanged.fromLeader() && ! stateChanged.toLeader()) {
            eventPublisher.publishEvent(new ClusterEvents.LeaderStepDown(stateChanged.getGroupId(), false));
        } else if( stateChanged.toLeader() && ! stateChanged.fromLeader()) {
            eventPublisher.publishEvent(new ClusterEvents.BecomeLeader(stateChanged.getGroupId(),
                                                                       node::unappliedEntries));
        } else if( stateChanged.toCandidate() ) {
            eventPublisher.publishEvent(new ClusterEvents.LeaderConfirmation(stateChanged.getGroupId(), null, false));
        } else if (!StringUtils.isEmpty(node.getLeaderName())) {
            eventPublisher.publishEvent(new ClusterEvents.LeaderConfirmation(stateChanged.getGroupId(),
                                                                             node.getLeaderName(),
                                                                             false));
        }
    }

    @EventListener
    public void on(ReplicationServerStarted replicationServerStarted) {
        this.replicationServerStarted = true;
        raftGroupMap.forEach((k,raftGroup) -> raftGroup.localNode().start());
    }

    public Collection<String> getContexts() {
        return raftGroupMap.keySet();
    }

    public RaftNode getRaftNode(String context) {
        if (!running) {
            throw new IllegalStateException("Initialization or shutdown in progress");
        }
        if( ! raftGroupMap.containsKey(context)) {
            throw new MessagingPlatformException(ErrorCode.CONTEXT_NOT_FOUND, messagingPlatformConfiguration.getName() + ": Not a member of " + context);

        }
        return raftGroupMap.get(context).localNode();
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    @Override
    public void stop(Runnable callback) {
        raftGroupMap.values().forEach(r -> r.localNode().stop());
        callback.run();
        running=false;

    }

    @Override
    public int getPhase() {
        return 100;
    }

    /**
     * Wait for the newly created raftgroup to be fully initialized and having a leader. Needs to wait until it has at least
     * applied the first entry (the fact that the current node is leader).
     * @param group the raft group
     * @return the raft node when it is leader
     */
    RaftNode waitForLeader(RaftGroup group) {
        while (! group.localNode().isLeader() || group.logEntryProcessor().lastAppliedIndex() == 0) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupt while waiting to become leader");
            }
        }
        return group.localNode();
    }

    /**
     * Finds a raft node. If it does not exists yet (node is just added to a raft group), it creates a new raft group on
     * this node.
     *
     * @param groupId the groupId of the raft group
     * @param nodeId  the id to use to register the current node if the group does not exist yet
     * @return the existing or newly created raft node
     */
    @Override
    public RaftNode getOrCreateRaftNode(String groupId, String nodeId) {
        RaftGroup raftGroup = raftGroupMap.get(groupId);
        if(raftGroup != null) return raftGroup.localNode();

        synchronized (raftGroupMap) {
            raftGroup = raftGroupMap.get(groupId);
            if(raftGroup == null) {
                raftGroup = createRaftGroup(groupId, nodeId, NO_EVENT_STORE, null);
            }
        }
        return raftGroup.localNode();
    }

    @Override
    public RaftNode raftNode(String groupId) {
        RaftGroup raftGroup = raftGroupMap.get(groupId);
        if (raftGroup != null) {
            return raftGroup.localNode();
        }
        return null;
    }

    /**
     * Scheduled job to persist Raft status every second. Storing on each change causes too much overhead with more than 100 transactions per second.
     */
    @Scheduled(fixedDelay = 1000)
    public void syncStore() {
        raftGroupMap.forEach((k,e) -> ((GrpcRaftGroup)e).syncStore());
    }


    /**
     * Returns the max election timeout
     * @return election timeout in ms.
     */
    public int electionTimeout() {
        return raftProperties.getMaxElectionTimeout();
    }
    /**
     * Retrieve all non-admin Contexts that this node is member of.
     * @return Iterable of context names
     */
    public Iterable<String> getStorageContexts() {
        return raftGroupNodeRepository.storageContexts();
    }

    public Set<String> raftGroups() {
        return raftGroupMap.keySet();
    }

    public RaftGroup getRaftGroup(String groupId) {
        return raftGroupMap.get(groupId);
    }

    public String getMyLabel(List<Node> raftNodes) {
        for (Node node :raftNodes) {
            if( node.getNodeName().equals(messagingPlatformConfiguration.getName()))
                return node.getNodeId();

        }
        throw new RuntimeException("Could not find current node in nodes");
    }

    public String getMyName() {
        return messagingPlatformConfiguration.getName();
    }

    public void delete(String context, boolean preserveEventStore) {
        raftGroupMap.remove(context);
        if( context.equals(getAdmin())) {
            eventPublisher.publishEvent(new ContextEvents.AdminContextDeleted(context));
        } else {
            eventPublisher.publishEvent(new ContextEvents.ContextDeleted(context, preserveEventStore));
        }
    }

    public void prepareDeleteNodeFromContext(String context, String node) {
        raftGroupNodeRepository.prepareDeleteNodeFromContext(context, node);
        eventPublisher.publishEvent(new ContextEvents.DeleteNodeFromContextRequested(context, node));
    }
}
