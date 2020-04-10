package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.election.ElectionStore;
import io.axoniq.axonserver.cluster.exception.ConcurrentMembershipStateModificationException;
import io.axoniq.axonserver.cluster.replication.EntryIterator;
import io.axoniq.axonserver.cluster.scheduler.DefaultScheduler;
import io.axoniq.axonserver.cluster.scheduler.ScheduledRegistration;
import io.axoniq.axonserver.cluster.scheduler.Scheduler;
import io.axoniq.axonserver.cluster.snapshot.SnapshotManager;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesRequest;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesResponse;
import io.axoniq.axonserver.grpc.cluster.Config;
import io.axoniq.axonserver.grpc.cluster.ConfigChangeResult;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotRequest;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotResponse;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.cluster.RequestVoteRequest;
import io.axoniq.axonserver.grpc.cluster.RequestVoteResponse;
import io.axoniq.axonserver.grpc.cluster.Role;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongSupplier;

import static java.lang.String.format;

/**
 * Entry point to clustering features.
 *
 * @author Allard Buijze
 * @since 4.1
 */
public class RaftNode {

    private static final Logger logger = LoggerFactory.getLogger(RaftNode.class);

    private final String nodeId;
    private final RaftGroup raftGroup;
    private final MembershipStateFactory stateFactory;
    private final AtomicReference<MembershipState> state = new AtomicReference<>();
    private final LogEntryApplier logEntryApplier;
    private volatile ScheduledRegistration scheduledLogCleaning;
    private final List<Consumer<StateChanged>> stateChangeListeners = new CopyOnWriteArrayList<>();
    private final List<BiConsumer<Long, String>> termChangeListeners = new CopyOnWriteArrayList<>();
    private final Scheduler scheduler;
    private final List<Consumer> messagesListeners = new CopyOnWriteArrayList<>();

    /**
     * Instantiates a Raft Node.
     *
     * @param nodeId                   the node identifier
     * @param raftGroup                the group this node belongs to
     * @param snapshotManager          manages snapshot creation/installation
     */
    public RaftNode(String nodeId, RaftGroup raftGroup, SnapshotManager snapshotManager) {
        this(nodeId, raftGroup, new DefaultScheduler(nodeId + "-raftNode"), snapshotManager);
    }

    /**
     * Instantiates a Raft Node.
     *
     * @param nodeId                   the node identifier
     * @param raftGroup                the group this node belongs to
     * @param scheduler                responsible for task scheduling
     * @param snapshotManager          manages snapshot creation/installation
     */
    public RaftNode(String nodeId, RaftGroup raftGroup, Scheduler scheduler, SnapshotManager snapshotManager) {
        this(nodeId, raftGroup, scheduler, snapshotManager, newConfiguration -> {});
    }

    /**
     * Instantiates a Raft Node.
     *
     * @param nodeId                   the node identifier
     * @param raftGroup                the group this node belongs to
     * @param scheduler                responsible for task scheduling
     * @param snapshotManager          manages snapshot creation/installation
     * @param newConfigurationConsumer consumes new configuration
     */
    public RaftNode(String nodeId, RaftGroup raftGroup, Scheduler scheduler, SnapshotManager snapshotManager,
                    NewConfigurationConsumer newConfigurationConsumer) {
        this.nodeId = nodeId;
        this.raftGroup = raftGroup;
        this.logEntryApplier = new LogEntryApplier(raftGroup,
                                                   scheduler,
                                                   e -> state.get().applied(e),
                                                   newConfiguration -> updateConfig(newConfiguration,
                                                                                    newConfigurationConsumer));
        stateFactory = new CachedStateFactory(new DefaultStateFactory(raftGroup, this::updateState,
                                                                      this::updateTerm, snapshotManager));
        this.scheduler = scheduler;
        updateState(null, stateFactory.idleState(nodeId), "Node initialized.");
    }

    private ScheduledRegistration scheduleLogCleaning() {
        return scheduler.scheduleWithFixedDelay(
                () -> {
                    logger.debug("{} in term {}: Clearing the log...", groupId(), currentTerm());
                    raftGroup.localLogEntryStore().clearOlderThan(raftGroup.raftConfiguration().logRetentionHours(),
                                                                  TimeUnit.HOURS,
                                                                  () -> raftGroup.logEntryProcessor()
                                                                                 .lastAppliedIndex());
                    logger.debug("{} in term {}: Log cleaned.", groupId(), currentTerm());
                },
                1,
                1,
                TimeUnit.HOURS);
    }

    /**
     * Checks if a log cleaning task is scheduled, cancels the current task and schedules a new one.
     * This ensures that the next cleaning is performed one hour later.
     *
     * @return {@code true} if the scheduled log cleaning task is restarted, {@code false} if no scheduled log cleaning
     * task was found.
     */
    public boolean restartLogCleaning() {
        logger.debug("{} in term {}: Restart log cleaning.", groupId(), currentTerm());
        if (stopLogCleaning()) {
            scheduledLogCleaning = scheduleLogCleaning();
            logger.debug("{} in term {}: Log cleaning restarted.", groupId(), currentTerm());
            return true;
        }
        logger.info("{} in term {}: Log cleaning couldn't be restarted because it has been stopped.",
                    groupId(),
                    currentTerm());
        return false;
    }

    /**
     * Performs a log compaction for logs older than the specified time.
     *
     * @param time     the time before which the logs could be deleted.
     * @param timeUnit the time unit
     */
    public void forceLogCleaning(long time, TimeUnit timeUnit) {
        logger.info("{} in term {}: Forcing log cleaning.", groupId(), currentTerm());
        LongSupplier lastAppliedIndexSupplier = () -> raftGroup.logEntryProcessor().lastAppliedIndex();
        raftGroup.localLogEntryStore().clearOlderThan(time, timeUnit, lastAppliedIndexSupplier);
        logger.info("{} in term {}: Log cleaned.", groupId(), currentTerm());
    }

    private boolean stopLogCleaning() {
        if (scheduledLogCleaning != null) {
            scheduledLogCleaning.cancel();
            scheduledLogCleaning = null;
            return true;
        }
        return false;
    }

    private void updateConfig(Config newConfiguration, NewConfigurationConsumer newConfigurationConsumer) {
        raftGroup.raftConfiguration().update(newConfiguration.getNodesList());
        newConfigurationConsumer.consume(newConfiguration);
        logger.debug("{} in term {}: Force refresh configuration",
                     groupId(),
                     currentTerm());
        state.get().currentConfiguration().refresh();
    }

    private synchronized void updateState(MembershipState currentState, MembershipState newState, String cause) {
        String newStateName = toString(newState);
        String currentStateName = toString(currentState);
        if (state.compareAndSet(currentState, newState)) {
            Optional.ofNullable(currentState).ifPresent(MembershipState::stop);
            logger.info("{} in term {}: Updating state from {} to {} ({})",
                        groupId(),
                        currentTerm(),
                        currentStateName,
                        newStateName,
                        cause);
            try {
                stateChangeListeners.forEach(stateChangeListeners -> {
                    try {
                        StateChanged change = new StateChanged(groupId(),
                                                               nodeId,
                                                               currentState,
                                                               newState,
                                                               cause,
                                                               currentTerm());
                        stateChangeListeners.accept(change);
                    } catch (Exception ex) {
                        logger.warn("{} in term {}: Failed to handle event", groupId(), currentTerm(), ex);
                        throw new RuntimeException("Transition to " + newStateName + " failed", ex);
                    }
                });
                newState.start();
            } catch (RuntimeException ex) {
                logger.warn(
                        "{} in term {}: transition to {} failed, moving to currentState (node: {}, currentState: {})",
                        groupId(),
                        currentTerm(),
                        newStateName,
                        nodeId,
                        currentStateName,
                        ex);
                newState.stop();
                MembershipState followerState = stateFactory.followerState();
                followerState.start();
                state.set(followerState);
                throw ex;
            }
        } else {
            logger.warn("{} in term {}: transition to {} failed, invalid current state (node: {}, currentState: {})",
                        groupId(),
                        currentTerm(),
                        newStateName,
                        nodeId,
                        currentStateName);
            throw new ConcurrentMembershipStateModificationException(String.format(
                    "invalid current state (currentState: %s, expectedCurrentState %s)",
                    currentStateName,
                    toString(state.get())));
        }
    }

    private synchronized void updateTerm(Long newTerm, String cause) {
        ElectionStore electionStore = raftGroup.localElectionStore();
        if (newTerm > electionStore.currentTerm()) {
            electionStore.updateCurrentTerm(newTerm);
            logger.info("{} in term {}: Term updated ({}).", groupId(), currentTerm(), cause);
            electionStore.markVotedFor(null);
            termChangeListeners.forEach(consumer -> consumer.accept(newTerm, cause));
        }
    }

    private String toString(MembershipState state) {
        return state == null ? null : state.getClass().getSimpleName();
    }

    /**
     * Starts this node.
     */
    public void start() {
        start(Role.PRIMARY);
    }

    public void start(Role role) {
        logger.info("{} in term {}: Starting the node...", groupId(), currentTerm());
        if (!state.get().isIdle()) {
            logger.debug("{} in term {}: Node is already started!", groupId(), currentTerm());
            return;
        }
        if (raftGroup.logEntryProcessor().lastAppliedIndex() > raftGroup.localLogEntryStore().lastLogIndex()) {
            logger.error(
                    "{} in term {}: Last applied index {} is higher than last log index {}",
                    groupId(),
                    currentTerm(),
                    raftGroup.logEntryProcessor().lastAppliedIndex(),
                    raftGroup.localLogEntryStore().lastLogIndex());
            if (raftGroup.localLogEntryStore().lastLogIndex() == 0) {
                raftGroup.logEntryProcessor().reset();
            }
        }

        moveToInitialState(role);

        logEntryApplier.start();
        if (raftGroup.raftConfiguration().isLogCompactionEnabled()) {
            scheduledLogCleaning = scheduleLogCleaning();
        }
        logger.info("{} in term {}: Node started.", groupId(), currentTerm());
    }

    private void moveToInitialState(Role role) {
        if (role == null) {
            updateState(state.get(), stateFactory.prospectState(), "Role unknown");
            return;
        }

        switch (role) {
            case PRIMARY:
                updateState(state.get(), stateFactory.followerState(), "Role " + role);
                break;
            case MESSAGING_ONLY:
            case ACTIVE_BACKUP:
            case PASSIVE_BACKUP:
                updateState(state.get(), stateFactory.secondaryState(), "Role " + role);
                break;
            default:
                updateState(state.get(), stateFactory.prospectState(), "Role unknown");
        }
    }

    /**
     * Register a state change listener.
     *
     * @param stateChangedConsumer a state change listener
     */
    // TODO: 2/28/2019 return a registration
    public void registerStateChangeListener(Consumer<StateChanged> stateChangedConsumer) {
        stateChangeListeners.add(stateChangedConsumer);
    }

    /**
     * Registers a term change listener.
     *
     * @param termChangedConsumer a term change listener
     */
    // TODO: 2/28/2019 return a registration
    public void registerTermChangeListener(BiConsumer<Long, String> termChangedConsumer) {
        termChangeListeners.add(termChangedConsumer);
    }

    /**
     * Handler of {@link AppendEntriesRequest} message.
     *
     * @param request entries that should be appended to our log
     * @return the response indicating the result of appending entries to our log
     */
    public synchronized AppendEntriesResponse appendEntries(AppendEntriesRequest request) {
        logger.trace("{} in term {}: Received AppendEntries request in state {} {}",
                     groupId(),
                     currentTerm(),
                     state.get(),
                     request);
        notifyMessage(request);
        AppendEntriesResponse response = runOnCurrentState(s -> s.appendEntries(request));
        notifyMessage(response);
        return response;
    }

    /**
     * Handler of {@link RequestVoteRequest} message.
     *
     * @param request to grant a vote to a candidate requesting it
     * @return the response indicating the result of vote granting
     */
    public synchronized RequestVoteResponse requestVote(RequestVoteRequest request) {
        logger.trace("{} in term {}: Received RequestVote request in state {} {}",
                     groupId(),
                     currentTerm(),
                     state.get(),
                     request);
        notifyMessage(request);
        RequestVoteResponse response = runOnCurrentState(s -> s.requestVote(request));
        notifyMessage(response);
        return response;
    }

    /**
     * Handles of {@link InstallSnapshotRequest} message.
     *
     * @param request to install the snapshot
     * @return the response indicating the result of snapshot installation
     */
    public synchronized InstallSnapshotResponse installSnapshot(InstallSnapshotRequest request) {
        logger.trace("{} in term {}: Received InstallSnapshot request in state {} {}",
                     groupId(),
                     currentTerm(),
                     state.get(),
                     request);
        notifyMessage(request);
        InstallSnapshotResponse response = runOnCurrentState(s -> s.installSnapshot(request));
        notifyMessage(response);
        return response;
    }

    private void notifyMessage(Object message) {
        messagesListeners.forEach(consumer -> {
            try {
                consumer.accept(message);
            } catch (Exception ignore) {
            }
        });
    }

    /**
     * Stops this node.
     */
    public void stop() {
        logger.info("{} in term {}: Stopping the node...", groupId(), currentTerm());
        stopLogCleaning();
        logEntryApplier.stop();
        updateState(state.get(), stateFactory.idleState(nodeId), "Node stopped");
        raftGroup.localLogEntryStore().close(false);
        logger.info("{} in term {}: Node stopped.", groupId(), currentTerm());
    }

    /**
     * Gets the id of this node.
     *
     * @return the id of this node
     */
    public String nodeId() {
        return nodeId;
    }

    /**
     * Am I the leader?
     *
     * @return {@code true} if I'm the leader, {@code false} otherwise
     */
    public boolean isLeader() {
        return state.get().isLeader();
    }

    /**
     * Registers a consumer for committed log entries.
     *
     * @param entryConsumer to consume committed log entries
     * @return a Runnable to be invoked in order to cancel this registration
     */
    public Runnable registerEntryConsumer(LogEntryConsumer entryConsumer) {
        return logEntryApplier.registerLogEntryConsumer(entryConsumer);
    }

    /**
     * Appends a given entry to a cluster of nodes.
     *
     * @param entryType the type of entry to be appended
     * @param entryData serialized binary array of entry to be appended
     * @return a Completable Future indicating the result of append
     */
    public CompletableFuture<Void> appendEntry(String entryType, byte[] entryData) {
        logger.trace("{} in term {}: append entry {}", groupId(), currentTerm(), entryType);
        return runOnCurrentState(s -> s.appendEntry(entryType, entryData));
    }

    /**
     * Adds a node to a cluster.
     *
     * @param node the node to be added
     * @return a Completable Future indicating the result of addition
     */
    public CompletableFuture<ConfigChangeResult> addNode(Node node) {
        logger.info("{} in term {}: Add a node {}.", groupId(), currentTerm(), node);
        return runOnCurrentState(s -> s.addServer(node));
    }

    /**
     * Removes a node from a cluster.
     *
     * @param nodeId the id of a node to be removed
     * @return a Completable Future indicating the result of removal
     */
    public CompletableFuture<ConfigChangeResult> removeNode(String nodeId) {
        logger.info("{} in term {}: Remove a node: {}.", groupId(), currentTerm(), nodeId);
        return runOnCurrentState(s -> s.removeServer(nodeId));
    }

    /**
     * Get the id of a group this node belongs to.
     *
     * @return the id of a group this node belongs to
     */
    public String groupId() {
        return raftGroup.raftConfiguration().groupId();
    }

    /**
     * Gets the current term this node is into.
     *
     * @return the current term this node is into
     */
    public long currentTerm() {
        return raftGroup.localElectionStore().currentTerm();
    }

    /**
     * Gets the leader of a cluster this node belongs to.
     *
     * @return the leader of a cluster this node belongs to
     */
    public String getLeader() {
        return state.get().getLeader();
    }

    /**
     * Forces this node to step down.
     */
    public void stepdown() {
        logger.info("{} in term {}: Stepping down started.", groupId(), currentTerm());
        runOnCurrentState(s -> {
            s.forceStartElection();
            return null;
        });
        logger.info("{} in term {}: Node stepped down.", groupId(), currentTerm());
    }

    /**
     * Stop accepting entries, complete replication to at least one peer and let peer start new election
     */
    public CompletableFuture<Void> transferLeadership() {
        logger.info("{} in term {}: Transfer leadership started.", groupId(), currentTerm());
        return runOnCurrentState(s -> s.transferLeadership());
    }

    /**
     * Gets the iterator of entries that have not been applied.
     *
     * @return the iterator of entries that have not been applied
     */
    public EntryIterator unappliedEntries() {
        return raftGroup.localLogEntryStore()
                        .createIterator(raftGroup.logEntryProcessor().lastAppliedIndex() + 1);
    }

    /**
     * Removes a group and stops this node.
     *
     * @return a Completable Future indicating the result of removal of a group
     */
    public CompletableFuture<Void> removeGroup() {
        logger.info("{} in term {}: Remove a group.", groupId(), currentTerm());
        stop();
        raftGroup.delete();
        scheduler.shutdown();
        logger.info("{} in term {}: Group removed.", groupId(), currentTerm());
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Gets the current cluster members of the group this node belongs to.
     *
     * @return list of nodes
     */
    public List<Node> currentGroupMembers() {
        return state.get().currentGroupMembers();
    }


    /**
     * Returns if the current configuration is pending (not yet committed).
     *
     * @return true if the current configuration is uncommitted, false otherwise
     */
    public boolean isCurrentConfigurationPending(){
        return state.get().currentConfiguration().isUncommitted();
    }

    /**
     * Gets the state factory.
     *
     * @return the state factory
     */
    // TODO: 2/28/2019 do we really want to expose this???
    public MembershipStateFactory stateFactory() {
        return stateFactory;
    }

    /**
     * Registers a listener for each and any request/response message to/from this node.
     *
     * @param messageConsumer the message consumer
     * @return a registration which can be used to unsubscribe this listener
     */
    public Registration registerMessageListener(Consumer messageConsumer) {
        this.messagesListeners.add(messageConsumer);
        return () -> this.messagesListeners.remove(messageConsumer);
    }

    /**
     * Gets the group this node belongs to.
     *
     * @return the group this node belongs to
     */
    public RaftGroup raftGroup() {
        return raftGroup;
    }

    /**
     * Gets the name of the leader of cluster this node belongs to.
     *
     * @return the node name
     */
    public String getLeaderName() {
        String leader = state.get().getLeader();
        if (leader == null) {
            return null;
        }
        return currentGroupMembers().stream()
                                    .filter(node -> node.getNodeId().equals(leader))
                                    .map(Node::getNodeName)
                                    .findFirst()
                                    .orElse(null);
    }

    /**
     * Gets the peers responsible for entry replication.
     *
     * @return Replicator Peer iterator
     */
    // TODO: 2/28/2019 do we really want to expose these???
    public Iterator<ReplicatorPeerStatus> replicatorPeers() {
        return state.get().replicatorPeers();
    }

    public void notifyNewLeader(String leaderId) {
        stateChangeListeners.forEach(stateChangeListeners -> {
            try {
                StateChanged change = new StateChanged(groupId(),
                                                       nodeId,
                                                       state.get(),
                                                       state.get(),
                                                       format("Leader changed to %s",leaderId),
                                                       currentTerm());
                stateChangeListeners.accept(change);
            } catch (Exception ex) {
                logger.warn("{} in term {}: Failed to handle event", groupId(), currentTerm(), ex);
            }
        });
    }

    public RequestVoteResponse requestPreVote(RequestVoteRequest request) {
        notifyMessage(request);
        RequestVoteResponse response = runOnCurrentState(s -> s.requestPreVote(request));
        notifyMessage(response);
        return response;
    }

    private <R> R runOnCurrentState(Function<MembershipState, R> action) {
        while (true) {
            try {
                return action.apply(state.get());
            } catch (ConcurrentMembershipStateModificationException cme) {

            }
        }
    }

    /**
     * Returns the number of unapplied but committed entries.
     *
     * @return the number of unapplied entries
     */
    public long unappliedEntriesCount() {
        return raftGroup.logEntryProcessor().commitIndex() - raftGroup.logEntryProcessor().lastAppliedIndex();
    }

    /**
     * Checks health of the node.
     *
     * @param statusConsumer consumer to provide status messages to
     * @return true if this node considers itself healthy
     */
    public boolean health(BiConsumer<String, String> statusConsumer) {
        return state.get() == null || state.get().health(statusConsumer);
    }

    /**
     * Forces node to move to fatal state.
     */
    public void changeToFatal() {
        updateState(state.get(), stateFactory.fatalState(), "Force to fatal state");
    }

    /**
     * Forces node to move to follower state.
     */
    public void changeToFollower() {
        updateState(state.get(), stateFactory.followerState(), "Force to fatal state");
    }
}
