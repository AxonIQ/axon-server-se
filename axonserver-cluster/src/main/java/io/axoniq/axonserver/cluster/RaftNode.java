package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.election.ElectionStore;
import io.axoniq.axonserver.cluster.replication.EntryIterator;
import io.axoniq.axonserver.cluster.scheduler.DefaultScheduler;
import io.axoniq.axonserver.cluster.scheduler.ScheduledRegistration;
import io.axoniq.axonserver.cluster.scheduler.Scheduler;
import io.axoniq.axonserver.cluster.snapshot.SnapshotManager;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesRequest;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesResponse;
import io.axoniq.axonserver.grpc.cluster.ConfigChangeResult;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotRequest;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotResponse;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.cluster.RequestVoteRequest;
import io.axoniq.axonserver.grpc.cluster.RequestVoteResponse;
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
    private final List<Consumer<Entry>> entryConsumer = new CopyOnWriteArrayList<>();
    private volatile ScheduledRegistration applyTask;
    private volatile ScheduledRegistration scheduledLogCleaning;
    private final List<Consumer<StateChanged>> stateChangeListeners = new CopyOnWriteArrayList<>();
    private final List<BiConsumer<Long, String>> termChangeListeners = new CopyOnWriteArrayList<>();
    private final Scheduler scheduler;
    private final List<Consumer> messagesListeners = new CopyOnWriteArrayList<>();

    /**
     * Instantiates a Raft Node.
     *
     * @param nodeId          the node identifier
     * @param raftGroup       the group this node belongs to
     * @param snapshotManager manages snapshot creation/installation
     */
    public RaftNode(String nodeId, RaftGroup raftGroup, SnapshotManager snapshotManager) {
        this(nodeId, raftGroup, new DefaultScheduler("raftNode-" + nodeId), snapshotManager);
    }

    /**
     * Instantiates a Raft Node.
     *
     * @param nodeId          the node identifier
     * @param raftGroup       the group this node belongs to
     * @param scheduler       responsible for task scheduling
     * @param snapshotManager manages snapshot creation/installation
     */
    public RaftNode(String nodeId, RaftGroup raftGroup, Scheduler scheduler, SnapshotManager snapshotManager) {
        this.nodeId = nodeId;
        this.raftGroup = raftGroup;
        this.registerEntryConsumer(this::updateConfig);
        stateFactory = new CachedStateFactory(new DefaultStateFactory(raftGroup, this::updateState,
                                                                      this::updateTerm, snapshotManager));
        this.scheduler = scheduler;
        updateState(null, stateFactory.idleState(nodeId), "Node initialized.");
    }

    private ScheduledRegistration scheduleLogCleaning() {
        return scheduler.scheduleWithFixedDelay(
                () -> {
                    logger.info("{} in term {}: Clearing the log...", groupId(), currentTerm());
                    raftGroup.localLogEntryStore().clearOlderThan(raftGroup.raftConfiguration().logRetentionHours(),
                                                                  TimeUnit.HOURS,
                                                                  () -> raftGroup.logEntryProcessor()
                                                                                 .lastAppliedIndex());
                    logger.info("{} in term {}: Log cleaned.", groupId(), currentTerm());
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
        logger.info("{} in term {}: Restart log cleaning.", groupId(), currentTerm());
        if (stopLogCleaning()) {
            scheduledLogCleaning = scheduleLogCleaning();
            logger.info("{} in term {}: Log cleaning restarted.", groupId(), currentTerm());
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

    private void updateConfig(Entry entry) {
        if (entry.hasNewConfiguration()) {
            raftGroup.raftConfiguration().update(entry.getNewConfiguration().getNodesList());
        }
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
            stateChangeListeners.forEach(stateChangeListeners -> {
                try {
                    StateChanged change = new StateChanged(groupId(),
                                                           nodeId,
                                                           currentStateName,
                                                           newStateName,
                                                           cause,
                                                           currentTerm());
                    stateChangeListeners.accept(change);
                } catch (Exception ex) {
                    logger.warn("{} in term {}: Failed to handle event", groupId(), currentTerm(), ex);
                    throw new RuntimeException("Transition to " + newStateName + " failed", ex);
                }
            });
            newState.start();
            logger.info("{} in term {}: Updated state to {}", groupId(), currentTerm(), newStateName);
        } else {
            logger.warn("{} in term {}: transition to {} failed, invalid current state (node: {}, currentState: {})",
                        groupId(),
                        currentTerm(),
                        newStateName,
                        nodeId,
                        currentStateName);
        }
    }

    private synchronized void updateTerm(Long newTerm, String cause) {
        ElectionStore electionStore = raftGroup.localElectionStore();
        if (newTerm > electionStore.currentTerm()) {
            electionStore.updateCurrentTerm(newTerm);
            logger.info("{} in term {}: Term updated.", groupId(), currentTerm());
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
        logger.info("{} in term {}: Starting the node...", groupId(), currentTerm());
        if (!state.get().isIdle()) {
            logger.warn("{} in term {}: Node is already started!", groupId(), currentTerm());
            return;
        }
        if (raftGroup.logEntryProcessor().lastAppliedIndex() > raftGroup.localLogEntryStore().lastLogIndex()) {
            logger.error(
                    "{} in term {}: Last applied index {} is higher than last log index {}",
                    groupId(),
                    currentTerm(),
                    raftGroup.logEntryProcessor().lastAppliedIndex(),
                    raftGroup.localLogEntryStore().lastLogIndex());
        }
        updateState(state.get(), stateFactory.followerState(), "Node started");
        applyTask = scheduler.scheduleWithFixedDelay(() -> raftGroup.logEntryProcessor()
                                                                    .apply(raftGroup
                                                                                   .localLogEntryStore()::createIterator,
                                                                           this::applyEntryConsumers),
                                                     0,
                                                     1,
                                                     TimeUnit.MILLISECONDS);
        if (raftGroup.raftConfiguration().isLogCompactionEnabled()) {
            scheduledLogCleaning = scheduleLogCleaning();
        }
        logger.info("{} in term {}: Node started.", groupId(), currentTerm());
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
        AppendEntriesResponse response = state.get().appendEntries(request);
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
        RequestVoteResponse response = state.get().requestVote(request);
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
        InstallSnapshotResponse response = state.get().installSnapshot(request);
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
        updateState(state.get(), stateFactory.idleState(nodeId), "Node stopped");
        if (applyTask != null) {
            applyTask.cancel(true);
            applyTask = null;
        }
        stopLogCleaning();
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
    public Runnable registerEntryConsumer(Consumer<Entry> entryConsumer) {
        this.entryConsumer.add(entryConsumer);
        return () -> this.entryConsumer.remove(entryConsumer);
    }

    private void applyEntryConsumers(Entry e) {
        logger.trace("{} in term {}: apply {}", groupId(), currentTerm(), e.getIndex());
        entryConsumer.forEach(consumer -> {
            try {
                consumer.accept(e);
            } catch (Exception ex) {
                logger.warn("{} in term {}: Error while applying entry {}",
                            groupId(),
                            currentTerm(),
                            e.getIndex(),
                            ex);
            }
        });
        state.get().applied(e);
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
        return state.get().appendEntry(entryType, entryData);
    }

    /**
     * Adds a node to a cluster.
     *
     * @param node the node to be added
     * @return a Completable Future indicating the result of addition
     */
    public CompletableFuture<ConfigChangeResult> addNode(Node node) {
        logger.info("{} in term {}: Add a node {}.", groupId(), currentTerm(), node);
        return state.get().addServer(node);
    }

    /**
     * Removes a node from a cluster.
     *
     * @param nodeId the id of a node to be removed
     * @return a Completable Future indicating the result of removal
     */
    public CompletableFuture<ConfigChangeResult> removeNode(String nodeId) {
        logger.info("{} in term {}: Remove a node: {}.", groupId(), currentTerm(), nodeId);
        return state.get().removeServer(nodeId);
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
        state.get().forceStepDown();
        logger.info("{} in term {}: Node stepped down.", groupId(), currentTerm());
    }

    /**
     * Stop accepting entries, complete replication to at least one peer and let peer start new election
     */
    public CompletableFuture<Void> transferLeadership() {
        logger.info("{} in term {}: Transfer leadership started.", groupId(), currentTerm());
        return state.get().transferLeadership();
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
    public Iterator<ReplicatorPeer> replicatorPeers() {
        return state.get().replicatorPeers();
    }

    public void receivedNewLeader(String leaderId) {
        stateChangeListeners.forEach(stateChangeListeners -> {
            try {
                StateChanged change = new StateChanged(groupId(),
                                                       nodeId,
                                                       toString(state.get()),
                                                       toString(state.get()),
                                                       format("Leader changed to %s",leaderId),
                                                       currentTerm());
                stateChangeListeners.accept(change);
            } catch (Exception ex) {
                logger.warn("{} in term {}: Failed to handle event", groupId(), currentTerm(), ex);
            }
        });
    }
}
