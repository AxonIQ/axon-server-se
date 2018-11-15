package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.cluster.Node;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class RaftNode {

    private final String nodeId;
    private final RaftGroup raftGroup;
    private final AtomicReference<MembershipState> state;
    private final List<Consumer<Entry>> entryConsumer = new CopyOnWriteArrayList<>();
    private final List<Registration> registrations = new CopyOnWriteArrayList<>();

    public RaftNode(String nodeId, RaftGroup raftGroup) {
        this.nodeId = nodeId;
        this.raftGroup = raftGroup;
        this.state = new AtomicReference<>(new IdleState(raftGroup, this::updateState));
    }

    private void updateState(MembershipState membershipState) {
        state.set(membershipState);
    }

    public void start() {
        state.get().start();

        registrations.add(raftGroup.onAppendEntries(state.get()::appendEntries));
        registrations.add(raftGroup.onInstallSnapshot(state.get()::installSnapshot));
        registrations.add(raftGroup.onRequestVote(state.get()::requestVote));
    }

    public void stop() {
        state.get().stop();
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
        throw new UnsupportedOperationException();
    }

    public CompletableFuture<Void> addNode(Node node) {
        throw new UnsupportedOperationException();
    }

    public CompletableFuture<Void> removeNode(String nodeId) {
        throw new UnsupportedOperationException();
    }
}
