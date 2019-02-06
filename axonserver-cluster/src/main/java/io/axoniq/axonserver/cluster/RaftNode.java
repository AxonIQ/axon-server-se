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

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

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
    private final List<BiConsumer<Long,String>> termChangeListeners = new CopyOnWriteArrayList<>();
    private final Scheduler scheduler;
    private final List<Consumer> messagesListeners = new CopyOnWriteArrayList<>();

    public RaftNode(String nodeId, RaftGroup raftGroup, SnapshotManager snapshotManager) {
        this(nodeId, raftGroup, new DefaultScheduler(), snapshotManager);
    }

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
                () -> raftGroup.localLogEntryStore().clearOlderThan(1,
                                                                    TimeUnit.HOURS,
                                                                    () -> raftGroup.logEntryProcessor().lastAppliedIndex()),
                1,
                1,
                TimeUnit.HOURS);
    }

    /**
     * Checks if a log cleaning task is scheduled, cancels the current task and schedules a new one.
     * This ensures that the next cleaning is performed one hour later.
     *
     * @return {@code true} if the scheduled log cleaning task is restarted, {@code false} if no scheduled log cleaning task was found.
     */
    public boolean restartLogCleaning() {
        if (stopLogCleaning()){
            scheduledLogCleaning = scheduleLogCleaning();
            return true;
        }
        return false;
    }


    private boolean stopLogCleaning(){
        if (scheduledLogCleaning != null) {
            scheduledLogCleaning.cancel();
            scheduledLogCleaning = null;
            return true;
        }
        return false;
    }

    private void updateConfig(Entry entry){
        if (entry.hasNewConfiguration()){
            raftGroup.raftConfiguration().update(entry.getNewConfiguration().getNodesList());
        }
    }

    private synchronized void updateState(MembershipState currentState, MembershipState newState, String cause) {
        String newStateName = toString(newState);
        String currentStateName = toString(currentState);
        long term = raftGroup.localElectionStore().currentTerm();
        if( state.compareAndSet(currentState, newState)) {
            Optional.ofNullable(currentState).ifPresent(MembershipState::stop);
            logger.info("{}: Updating state from {} to {} ({})", groupId(), currentStateName, newStateName, cause);
            stateChangeListeners.forEach(stateChangeListeners -> {
                try {
                    StateChanged change = new StateChanged(groupId(), nodeId, currentStateName, newStateName, cause, term);
                    stateChangeListeners.accept(change);
                } catch (Exception ex) {
                    logger.warn("{}: Failed to handle event", groupId(), ex);
                    throw new RuntimeException("Transition to " + newStateName + " failed", ex);
                }
            });
            logger.info("{}: Updated state to {}", groupId(), newStateName);
            newState.start();
        } else {
            logger.warn("{}: transition to {} failed, invalid current state (node: {}, term:{}, currentState: {})",
                        groupId(), newStateName, nodeId, term, currentStateName);
        }
    }

    private synchronized void updateTerm(Long newTerm, String cause){
        ElectionStore electionStore = raftGroup.localElectionStore();
        if (newTerm > electionStore.currentTerm()) {
            electionStore.updateCurrentTerm(newTerm);
            electionStore.markVotedFor(null);
            termChangeListeners.forEach(consumer -> consumer.accept(newTerm, cause));
        }
    }

    private String toString(MembershipState state) {
        return state == null? null : state.getClass().getSimpleName();
    }

    public void start() {
        logger.info("{}: Starting the node...", groupId());
        if (!state.get().isIdle()) {
            logger.warn("{}: Node is already started!", groupId());
            return;
        }
        updateState(state.get(), stateFactory.followerState(), "Node started");
        applyTask = scheduler.scheduleWithFixedDelay( () -> raftGroup.logEntryProcessor()
                                                   .apply(raftGroup.localLogEntryStore()::createIterator,
                                                          this::applyEntryConsumers), 0, 1, TimeUnit.MILLISECONDS);
        if (raftGroup.raftConfiguration().isLogCompactionEnabled()){
            scheduledLogCleaning = scheduleLogCleaning();
        }
        logger.info("{}: Node started.", groupId());
    }

    public void registerStateChangeListener(Consumer<StateChanged> stateChangedConsumer) {
        stateChangeListeners.add(stateChangedConsumer);
    }

    public void registerTermChangeListener(BiConsumer<Long,String> termChangedConsumer) {
        termChangeListeners.add(termChangedConsumer);
    }

    public synchronized AppendEntriesResponse appendEntries(AppendEntriesRequest request) {
        logger.trace("{}: Received AppendEntries request in state {} {}", groupId(), state.get(), request);
        messagesListeners.forEach(consumer -> consumer.accept(request));
        AppendEntriesResponse response = state.get().appendEntries(request);
        messagesListeners.forEach(consumer -> consumer.accept(response));
        return response;
    }

    public synchronized RequestVoteResponse requestVote(RequestVoteRequest request) {
        logger.trace("{}: Received RequestVote request in state {} {}", groupId(), state.get(), request);
        messagesListeners.forEach(consumer -> consumer.accept(request));
        RequestVoteResponse response = state.get().requestVote(request);
        messagesListeners.forEach(consumer -> consumer.accept(response));
        return response;
    }

    public synchronized InstallSnapshotResponse installSnapshot(InstallSnapshotRequest request) {
        logger.trace("{}: Received InstallSnapshot request in state {} {}", groupId(), state.get(), request);
        messagesListeners.forEach(consumer -> consumer.accept(request));
        InstallSnapshotResponse response = state.get().installSnapshot(request);
        messagesListeners.forEach(consumer -> consumer.accept(response));
        return response;
    }

    public void stop() {
        logger.info("{}: Stopping the node...", groupId());
        updateState(state.get(), stateFactory.idleState(nodeId), "Node stopped");
        logger.info("{}: Moved to idle state", groupId());
        if (applyTask != null) {
            applyTask.cancel(true);
            applyTask = null;
        }
        stopLogCleaning();
        logger.info("{}: Node stopped.", groupId());
    }

    public String nodeId() {
        return nodeId;
    }

    public boolean isLeader() {
        return state.get().isLeader();
    }

    public Runnable registerEntryConsumer(Consumer<Entry> entryConsumer) {
        this.entryConsumer.add(entryConsumer);
        return () -> this.entryConsumer.remove(entryConsumer);
    }

    private void applyEntryConsumers(Entry e) {
        logger.trace("{}: apply {}", nodeId, e.getIndex());
        entryConsumer.forEach(consumer -> {
            try {
                consumer.accept(e);
            } catch (Exception ex) {
                logger.warn("{}: Error while applying entry {}: {}", groupId(), e.getIndex(), e, ex);
            }
        });
        state.get().applied(e);
    }


    public CompletableFuture<Void> appendEntry(String entryType, byte[] entryData) {
        logger.trace("{}: append entry {}", nodeId, entryType);
        return state.get().appendEntry(entryType, entryData);
    }

    public CompletableFuture<ConfigChangeResult> addNode(Node node) {
        return state.get().addServer(node);
    }

    public CompletableFuture<ConfigChangeResult> removeNode(String nodeId) {
        return state.get().removeServer(nodeId);
    }

    public String groupId() {
        return raftGroup.raftConfiguration().groupId();
    }

    public String getLeader() {
        return state.get().getLeader();
    }

    public void stepdown() {
        state.get().forceStepDown();
    }

    public EntryIterator unappliedEntries() {
        return raftGroup.localLogEntryStore().createIterator(raftGroup.logEntryProcessor().lastAppliedIndex() + 1);
    }

    public CompletableFuture<Void> removeGroup() {
        state.get().stop();
        raftGroup.delete();
        return CompletableFuture.completedFuture(null);
    }

    public List<Node> currentGroupMembers(){
        return state.get().currentGroupMembers();
    }

    public MembershipStateFactory stateFactory() {
        return stateFactory;
    }

    public Registration registerMessageListener(Consumer messageConsumer){
        this.messagesListeners.add(messageConsumer);
        return () -> this.messagesListeners.remove(messageConsumer);
    }

    public RaftGroup raftGroup() {
        return raftGroup;
    }
}
