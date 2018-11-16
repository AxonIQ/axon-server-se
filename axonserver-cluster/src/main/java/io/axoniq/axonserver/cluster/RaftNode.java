package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.grpc.cluster.AppendEntriesRequest;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesResponse;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotRequest;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotResponse;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.cluster.RequestVoteRequest;
import io.axoniq.axonserver.grpc.cluster.RequestVoteResponse;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class RaftNode {

    private final String nodeId;
    private final RaftGroup raftGroup;
    private final MembershipStateFactory stateFactory;
    private final AtomicReference<MembershipState> state = new AtomicReference<>();
    private final List<Consumer<Entry>> entryConsumer = new CopyOnWriteArrayList<>();
    private final List<Registration> registrations = new CopyOnWriteArrayList<>();

    public RaftNode(String nodeId, RaftGroup raftGroup) {
        this.nodeId = nodeId;
        this.raftGroup = raftGroup;
        stateFactory = new DefaultStateFactory(raftGroup, this::updateState);
        updateState(stateFactory.idleState());
    }

    private synchronized void updateState(MembershipState newState) {
        Optional.ofNullable(state.get()).ifPresent(MembershipState::stop);
        state.set(newState);
        newState.start();
    }

    public void start() {
        updateState(stateFactory.followerState());
        registrations.add(raftGroup.onAppendEntries(this::appendEntries));
        registrations.add(raftGroup.onInstallSnapshot(this::installSnapshot));
        registrations.add(raftGroup.onRequestVote(this::requestVote));
    }

    private synchronized AppendEntriesResponse appendEntries(AppendEntriesRequest request) {
        return state.get().appendEntries(request);
    }

    private synchronized RequestVoteResponse requestVote(RequestVoteRequest request) {
        return state.get().requestVote(request);
    }

    private synchronized InstallSnapshotResponse installSnapshot(InstallSnapshotRequest request) {
        return state.get().installSnapshot(request);
    }


    public void stop() {
        updateState(stateFactory.idleState());
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

    public CompletableFuture<Void> appendEntry(String entryType, byte[] entryData) {
        return state.get().appendEntry(entryType, entryData);
    }

    public CompletableFuture<Void> addNode(Node node) {
        throw new UnsupportedOperationException();
    }

    public CompletableFuture<Void> removeNode(String nodeId) {
        throw new UnsupportedOperationException();
    }
}
