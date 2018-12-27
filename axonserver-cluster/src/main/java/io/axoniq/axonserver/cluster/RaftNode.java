package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.replication.EntryIterator;
import io.axoniq.axonserver.cluster.util.AxonThreadFactory;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class RaftNode {

    private final ExecutorService executor = Executors.newCachedThreadPool(new AxonThreadFactory("Apply"));

    private static final Logger logger = LoggerFactory.getLogger(RaftNode.class);

    private final String nodeId;
    private final RaftGroup raftGroup;
    private final MembershipStateFactory stateFactory;
    private final AtomicReference<MembershipState> state = new AtomicReference<>();
    private final List<Consumer<Entry>> entryConsumer = new CopyOnWriteArrayList<>();
    private final List<Registration> registrations = new CopyOnWriteArrayList<>();
    private volatile Future<?> applyTask;
    private final List<Consumer<StateChanged>> stateChangeListeners = new CopyOnWriteArrayList<>();
    private final Scheduler scheduler;

    public RaftNode(String nodeId, RaftGroup raftGroup) {
        this(nodeId, raftGroup, new DefaultScheduler());
    }

    public RaftNode(String nodeId, RaftGroup raftGroup, Scheduler scheduler) {
        this.nodeId = nodeId;
        this.raftGroup = raftGroup;
        this.registerEntryConsumer(this::updateConfig);
        stateFactory = new CachedStateFactory(new DefaultStateFactory(raftGroup, this::updateState));
        this.scheduler = scheduler;
        updateState(null, stateFactory.idleState(nodeId));
        scheduleLogCleaning();
    }

    private void scheduleLogCleaning() {
        scheduler.scheduleWithFixedDelay(() -> raftGroup.localLogEntryStore().clearOlderThan(1, TimeUnit.HOURS),
                                         0,
                                         1,
                                         TimeUnit.HOURS);
    }

    private void updateConfig(Entry entry){
        if (entry.hasNewConfiguration()){
            raftGroup.raftConfiguration().update(entry.getNewConfiguration().getNodesList());
        }
    }

    private synchronized void updateState(MembershipState currentState, MembershipState newState) {
        if( state.compareAndSet(currentState, newState)) {
            Optional.ofNullable(currentState).ifPresent(MembershipState::stop);
            logger.info("{}: Updating state from {} to {}", groupId(), toString(currentState), toString(newState));
            stateChangeListeners.forEach(stateChangeListeners -> {
                try {
                    stateChangeListeners.accept(new StateChanged(toString(currentState),
                                                                 toString(newState),
                                                                 groupId()));
                } catch (Exception ex) {
                    logger.warn("{}: Failed to handle event", groupId(), ex);
                    throw new RuntimeException("Transition to " + toString(newState) + " failed", ex);
                }
            });
            logger.info("{}: Updated state to {}", groupId(), toString(newState));
            newState.start();
        } else {
            logger.warn("{}: transition to {} failed, invalid current state", groupId(), toString(newState));
        }
    }

    private String toString(MembershipState state) {
        return state == null? null : state.getClass().getSimpleName();
    }

    public void start() {
        updateState(state.get(), stateFactory.followerState());
        applyTask = executor.submit(() -> raftGroup.logEntryProcessor()
                                                   .start(raftGroup.localLogEntryStore()::createIterator,
                                                          this::applyEntryConsumers));
    }

    public void registerStateChangeListener(Consumer<StateChanged> stateChangedConsumer) {
        stateChangeListeners.add(stateChangedConsumer);
    }

    public synchronized AppendEntriesResponse appendEntries(AppendEntriesRequest request) {
        return state.get().appendEntries(request);
    }

    public synchronized RequestVoteResponse requestVote(RequestVoteRequest request) {
        return state.get().requestVote(request);
    }

    public synchronized InstallSnapshotResponse installSnapshot(InstallSnapshotRequest request) {
        return state.get().installSnapshot(request);
    }

    public void stop() {
        raftGroup.logEntryProcessor().stop();
        applyTask.cancel(true);
        applyTask = null;
        updateState(state.get(), stateFactory.idleState(nodeId));
        registrations.forEach(Registration::cancel);
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
        raftGroup.raftConfiguration().clear();
        return CompletableFuture.completedFuture(null);
    }

    public List<Node> currentGroupMembers(){
        return state.get().currentGroupMembers();
    }
}
