package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.grpc.cluster.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class RaftNode {
    private final ExecutorService executor = Executors.newCachedThreadPool(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            Thread t= new Thread(r);
            t.setName("Replication-" + raftGroup.raftConfiguration().groupId());
            return t;
        }
    });

    private static final Logger logger = LoggerFactory.getLogger(RaftNode.class);

    private final String nodeId;
    private final RaftGroup raftGroup;
    private final MembershipStateFactory stateFactory;
    private final AtomicReference<MembershipState> state = new AtomicReference<>();
    private final List<Consumer<Entry>> entryConsumer = new CopyOnWriteArrayList<>();
    private final List<Registration> registrations = new CopyOnWriteArrayList<>();
    private volatile Future<?> applyTask;
    private volatile boolean running;

    public RaftNode(String nodeId, RaftGroup raftGroup) {
        this.nodeId = nodeId;
        this.raftGroup = raftGroup;
        stateFactory = new DefaultStateFactory(raftGroup, this::updateState);
        updateState(stateFactory.idleState());
    }

    private synchronized void updateState(MembershipState newState) {
        logger.info("Updating state of {} to {}", nodeId, newState.getClass().getSimpleName());
        Optional.ofNullable(state.get()).ifPresent(MembershipState::stop);
        state.set(newState);
        newState.start();
    }

    public void start() {
        running = true;
        updateState(stateFactory.followerState());
        applyTask = executor.submit(() -> applyEntries());
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
        running = false;
        applyTask.cancel(true);
        applyTask = null;
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

    private void applyEntries() {
        while(running) {
            int retries = 10;
            while( retries > 0) {
                int applied = raftGroup.localLogEntryStore().applyEntries(e -> applyEntryConsumers(e));
                if( applied > 0 ) {
                    retries = 0;
                } else {
                    retries--;
                }

                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    running = false;
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private void applyEntryConsumers(Entry e) {
        entryConsumer.forEach(consumer -> consumer.accept(e));
        state.get().applied(e);
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

    public String groupId() {
        return raftGroup.raftConfiguration().groupId();
    }
}
