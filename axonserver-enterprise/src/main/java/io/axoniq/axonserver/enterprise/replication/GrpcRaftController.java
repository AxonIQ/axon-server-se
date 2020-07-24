package io.axoniq.axonserver.enterprise.replication;

import io.axoniq.axonserver.cluster.RaftGroup;
import io.axoniq.axonserver.cluster.RaftNode;
import io.axoniq.axonserver.cluster.StateChanged;
import io.axoniq.axonserver.cluster.grpc.RaftGroupManager;
import io.axoniq.axonserver.cluster.jpa.ReplicationGroupMember;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.cluster.GrpcRaftGroupFactory;
import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import io.axoniq.axonserver.enterprise.cluster.internal.ReplicationServerStarted;
import io.axoniq.axonserver.enterprise.config.RaftProperties;
import io.axoniq.axonserver.enterprise.replication.group.ReplicationGroupController;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.cluster.Role;
import io.axoniq.axonserver.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Controller;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Collections.singletonList;

/**
 * @author Marc Gathier
 */
@Controller
public class GrpcRaftController implements SmartLifecycle, RaftGroupManager {

    private final Logger logger = LoggerFactory.getLogger(GrpcRaftController.class);
    private final MessagingPlatformConfiguration messagingPlatformConfiguration;
    private final Map<String, RaftGroup> raftGroupMap = new ConcurrentHashMap<>();
    private final RaftGroupRepositoryManager raftGroupNodeRepository;
    private final ReplicationGroupController replicationGroupController;
    private final RaftProperties raftProperties;
    private final ApplicationEventPublisher eventPublisher;
    private final GrpcRaftGroupFactory groupFactory;
    private final Map<String, Long> blacklistedGroups = new ConcurrentHashMap<>();
    private boolean running;
    private volatile boolean replicationServerStarted;

    public GrpcRaftController(MessagingPlatformConfiguration messagingPlatformConfiguration,
                              RaftProperties raftProperties,
                              RaftGroupRepositoryManager raftGroupNodeRepository,
                              ApplicationEventPublisher eventPublisher,
                              ReplicationGroupController replicationGroupController,
                              GrpcRaftGroupFactory groupFactory) {
        this.messagingPlatformConfiguration = messagingPlatformConfiguration;
        this.raftGroupNodeRepository = raftGroupNodeRepository;
        this.replicationGroupController = replicationGroupController;
        this.raftProperties = raftProperties;
        this.eventPublisher = eventPublisher;
        this.groupFactory = groupFactory;
    }


    public void start() {
        if (raftProperties.getSegmentSize() < messagingPlatformConfiguration.getMaxMessageSize() * 2) {
            raftProperties.setSegmentSize(messagingPlatformConfiguration.getMaxMessageSize() * 2);
            logger.warn("Updating replication segment size (axoniq.axonserver.replication.segment-size) to {}",
                        raftProperties.getSegmentSize());
        }
        Set<ReplicationGroupMember> groups = raftGroupNodeRepository.getMyReplicationGroups();
        groups.forEach(context -> {
            try {
                createRaftGroup(context.getGroupId(), context.getNodeId(), context.getRole());
            } catch (Exception ex) {
                logger.warn("{}: Failed to initialize context", context.getGroupId(), ex);
            }
        });
        running = true;
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
        RaftGroup raftGroup = createRaftGroup(groupId, nodeLabel, Role.PRIMARY);
        raftGroup.raftConfiguration().update(singletonList(node));
        if (replicationServerStarted) {
            raftGroup.connect();
        }
        return raftGroup;
    }

    private RaftGroup createRaftGroup(String groupId, String localNodeId, Role role) {
        synchronized (raftGroupMap) {
            RaftGroup existingRaftGroup = raftGroupMap.get(groupId);
            if (existingRaftGroup != null) {
                return existingRaftGroup;
            }

            RaftGroup raftGroup = groupFactory.create(groupId, localNodeId);

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
        if (stateChanged.fromLeader() && !stateChanged.toLeader()) {
            eventPublisher.publishEvent(new ClusterEvents.LeaderStepDown(stateChanged.getGroupId()));
        } else if (stateChanged.toLeader() && !stateChanged.fromLeader()) {
            eventPublisher.publishEvent(new ClusterEvents.BecomeLeader(stateChanged.getGroupId(),
                                                                       node::unappliedEntries));
        } else if (stateChanged.toCandidate()) {
            eventPublisher.publishEvent(new ClusterEvents.LeaderConfirmation(stateChanged.getGroupId(), null));
        } else if (!StringUtils.isEmpty(node.getLeaderName())) {
            eventPublisher.publishEvent(new ClusterEvents.LeaderConfirmation(stateChanged.getGroupId(),
                                                                             node.getLeaderName()));
        }
    }

    @EventListener
    public void on(ReplicationServerStarted replicationServerStarted) {
        this.replicationServerStarted = true;
        raftGroupMap.forEach((k, raftGroup) -> raftGroup.localNode().start());
    }

    public Collection<String> getRaftGroups() {
        return raftGroupMap.keySet();
    }

    public RaftNode getRaftNode(String context) {
        if (!running) {
            throw new IllegalStateException("Initialization or shutdown in progress");
        }
        if (!raftGroupMap.containsKey(context)) {
            throw new MessagingPlatformException(ErrorCode.CONTEXT_NOT_FOUND,
                                                 messagingPlatformConfiguration.getName() + ": Not a member of "
                                                         + context);
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
        running = false;
    }

    @Override
    public int getPhase() {
        return 100;
    }

    /**
     * Wait for the newly created raftgroup to be fully initialized and having a leader. Needs to wait until it has at
     * least
     * applied the first entry (the fact that the current node is leader).
     *
     * @param group the raft group
     * @return the raft node when it is leader
     */
    public RaftNode waitForLeader(RaftGroup group) {
        while (!group.localNode().isLeader() || group.logEntryProcessor().lastAppliedIndex() == 0) {
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
        if (raftGroup != null) {
            return raftGroup.localNode();
        }

        synchronized (raftGroupMap) {
            raftGroup = raftGroupMap.get(groupId);
            if (raftGroup == null) {
                if (blacklisted(groupId)) {
                    throw new MessagingPlatformException(ErrorCode.CONTEXT_NOT_FOUND, "Context deletion in progress");
                }
                raftGroup = createRaftGroup(groupId, nodeId, null);
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
     * Scheduled job to persist Raft status every second. Storing on each change causes too much overhead with more than
     * 100 transactions per second.
     */
    @Scheduled(fixedDelay = 1000)
    public void syncStore() {
        if (running) {
            raftGroupMap.forEach((k, e) -> ((GrpcRaftGroup) e).syncStore());
        }
    }


    /**
     * Returns the max election timeout
     *
     * @return election timeout in ms.
     */
    public int electionTimeout() {
        return raftProperties.getMaxElectionTimeout();
    }

    /**
     * Retrieve all non-admin Contexts that this node is member of.
     *
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
        for (Node node : raftNodes) {
            if (node.getNodeName().equals(messagingPlatformConfiguration.getName())) {
                return node.getNodeId();
            }
        }
        throw new RuntimeException("Could not find current node in nodes");
    }

    public String getMyName() {
        return messagingPlatformConfiguration.getName();
    }

    public void delete(String replicationGroup, boolean preserveEventStore) {
        blacklist(replicationGroup);
        raftGroupMap.remove(replicationGroup);
        replicationGroupController.deleteReplicationGroup(replicationGroup, preserveEventStore);
        eventPublisher.publishEvent(new ClusterEvents.ReplicationGroupDeleted(replicationGroup, preserveEventStore));
    }

    public void prepareDeleteNodeFromReplicationGroup(String replicationGroup, String node) {
        raftGroupNodeRepository.prepareDeleteNodeFromReplicationGroup(replicationGroup, node);
        eventPublisher.publishEvent(new ClusterEvents.DeleteNodeFromReplicationGroupRequested(replicationGroup, node));
    }

    private void blacklist(String groupId) {
        blacklistedGroups.put(groupId, System.currentTimeMillis() + 2 * raftProperties.getMaxElectionTimeout());
    }

    private boolean blacklisted(String groupId) {
        return blacklistedGroups.getOrDefault(groupId, 0L) > System.currentTimeMillis();
    }


    public RaftNode getRaftNodeForContext(String context) {
        return replicationGroupController.findReplicationGroupByContext(context)
                                         .map(this::getRaftNode)
                                         .orElseThrow(() -> new MessagingPlatformException(ErrorCode.CONTEXT_NOT_FOUND,
                                                                                           messagingPlatformConfiguration
                                                                                                   .getName()
                                                                                                   + ": Not a member of "
                                                                                                   + context));
    }
}
