package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.util.AxonThreadFactory;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesRequest;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesResponse;
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

    public RaftNode(String nodeId, RaftGroup raftGroup) {
        this.nodeId = nodeId;
        this.raftGroup = raftGroup;
        stateFactory = new CachedStateFactory(new DefaultStateFactory(raftGroup, this::updateState));
        updateState(stateFactory.idleState(nodeId));
    }

    private synchronized void updateState(MembershipState newState) {
        Optional.ofNullable(state.get()).ifPresent(MembershipState::stop);
        logger.debug("Updating state of {} from {} to {}", nodeId, state.get(), newState);
        state.set(newState);
        newState.start();
    }

    public void start() {
        updateState(stateFactory.followerState());
        applyTask = executor.submit(() -> raftGroup.logEntryProcessor().start(raftGroup.localLogEntryStore()::createIterator, this::applyEntryConsumers));
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
        updateState(stateFactory.idleState(nodeId));
        registrations.forEach(Registration::cancel);
    }

    String nodeId() {
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
        entryConsumer.forEach(consumer -> consumer.accept(e));
        state.get().applied(e);
    }


    public CompletableFuture<Void> appendEntry(String entryType, byte[] entryData) {
        logger.trace("{}: append entry {}", nodeId, entryType);
        return state.get().appendEntry(entryType, entryData);
    }

    public CompletableFuture<Void> addNode(Node node) {
        return state.get().registerNode(node);
    }

    public CompletableFuture<Void> removeNode(String nodeId) {
        return state.get().unregisterNode(nodeId);
    }

    public String groupId() {
        return raftGroup.raftConfiguration().groupId();
    }

    public String getLeader() {
        return state.get().getLeader();
    }
}
