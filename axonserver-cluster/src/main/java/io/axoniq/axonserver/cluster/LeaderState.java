package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.Scheduler.ScheduledRegistration;
import io.axoniq.axonserver.cluster.util.AxonThreadFactory;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesRequest;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesResponse;
import io.axoniq.axonserver.grpc.cluster.Config;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotRequest;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotResponse;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.cluster.RequestVoteRequest;
import io.axoniq.axonserver.grpc.cluster.RequestVoteResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Author: marc
 */
public class LeaderState extends AbstractMembershipState {
    private static final long FLOW_BUFFER = 1000;
    private static final long MAX_ENTRIES_PER_BATCH = 100;

    private static final Logger logger = LoggerFactory.getLogger(LeaderState.class);
    private final AtomicReference<ScheduledRegistration> stepDown = new AtomicReference<>();

    private final NavigableMap<Long, CompletableFuture<Void>> pendingEntries = new ConcurrentSkipListMap<>();
    private final ExecutorService executor;
    private volatile Replicators replicators;
    private final Clock clock;

    protected static class Builder extends AbstractMembershipState.Builder<Builder> {
        public LeaderState build(){
            return new LeaderState(this);
        }
    }

    public static Builder builder(){
        return new Builder();
    }

    private LeaderState(Builder builder) {
        super(builder);
        executor = Executors.newCachedThreadPool(new AxonThreadFactory("Replicator-" + groupId()));
        clock = scheduler().clock();
    }

    @Override
    public void start() {
        scheduleStepDown();
        replicators = new Replicators();
        executor.submit(() -> replicators.start());
    }

    @Override
    public void stop() {
        replicators.stop();
        replicators = null;
        cancelStepDown();
        pendingEntries.forEach((index, completableFuture) -> completableFuture.completeExceptionally(new IllegalStateException()));
        pendingEntries.clear();
    }

    @Override
    public synchronized AppendEntriesResponse appendEntries(AppendEntriesRequest request) {
        if (request.getTerm() > currentTerm()) {
            logger.info("{}: received higher term from {}", groupId(), request.getLeaderId());
            return handleAsFollower(follower -> follower.appendEntries(request));
        }
        return appendEntriesFailure();
    }

    @Override
    public synchronized RequestVoteResponse requestVote(RequestVoteRequest request) {
        return requestVoteResponse(false);
    }

    @Override
    public synchronized InstallSnapshotResponse installSnapshot(InstallSnapshotRequest request) {
        if (request.getTerm() > currentTerm()) {
            return handleAsFollower(follower -> follower.installSnapshot(request));
        }
        return installSnapshotFailure();
    }

    @Override
    public boolean isLeader() {
        return true;
    }

    @Override
    public CompletableFuture<Void> appendEntry(String entryType, byte[] entryData) {
        return createEntry(currentTerm(), entryType, entryData);
    }

    private void scheduleStepDown() {
        ScheduledRegistration newTask = scheduler().schedule(this::checkStepdown, maxElectionTimeout(), MILLISECONDS);
        stepDown.set(newTask);
    }

    private void cancelStepDown(){
        stepDown.get().cancel();
    }

    public void forceStepDown() {
        logger.info("{}: StepDown forced", groupId());
        changeStateTo(stateFactory().followerState());
    }

    private void checkStepdown() {
        if( otherNodes().isEmpty()) return;
        long now = clock.millis();
        long lastReceived = replicators.lastMessageTimeFromMajority();
        if( now - lastReceived > maxElectionTimeout()) {
            logger.info("{}: StepDown as no messages received for {}ms", groupId(), (now-lastReceived));
            changeStateTo(stateFactory().followerState());
        } else {
            logger.trace("{}: Reschedule checkStepdown after {}ms", groupId(), maxElectionTimeout() - (now-lastReceived));
            ScheduledRegistration newTask = scheduler().schedule(this::checkStepdown, maxElectionTimeout() - (now-lastReceived), MILLISECONDS);
            stepDown.set(newTask);
        }

    }

    private CompletableFuture<Void> createEntry(long currentTerm, String entryType, byte[] entryData) {
        CompletableFuture<Void> appendEntryDone = new CompletableFuture<>();
        if( replicators == null) {
            appendEntryDone.completeExceptionally(new RuntimeException("Step down in progress"));
            return appendEntryDone;
        }
        CompletableFuture<Entry> entryFuture = raftGroup().localLogEntryStore().createEntry(currentTerm, entryType, entryData);
        entryFuture.whenComplete((e, failure) -> {
            if( failure != null) {
                appendEntryDone.completeExceptionally(failure);
            } else {
                if( replicators != null) {
                    replicators.notifySenders(e);
                }
                pendingEntries.put(e.getIndex(), appendEntryDone);
                replicators.updateMatchIndex(e.getIndex());
            }
        });
        return appendEntryDone;
    }

    @Override
    public void applied(Entry e) {
        Map.Entry<Long, CompletableFuture<Void>> first = pendingEntries.pollFirstEntry();
        boolean found = false;
        while( first != null && first.getKey() <= e.getIndex()) {
            first.getValue().complete(null);
            found = e.getIndex() == first.getKey();
            first = pendingEntries.pollFirstEntry();
        }

        if( !found && logger.isTraceEnabled()) {
            logger.trace("{}: entry not found when applied {} - {}", groupId(), e.getIndex(), pendingEntries.keySet());
        }
        if( first != null) {
            pendingEntries.put(first.getKey(), first.getValue());
        }
    }

    @Override
    public String getLeader() {
        return me();
    }

    @Override
    public CompletableFuture<Void> registerNode(Node node) {
        raftGroup().registerNode(node);
        replicators.addNode(node);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> unregisterNode(String nodeId) {
        raftGroup().unregisterNode(nodeId);
        replicators.removeNode(nodeId);
        return CompletableFuture.completedFuture(null);
    }

    private void appendConfiguration() {
        Config config = Config.newBuilder().addAllNodes(raftGroup().raftConfiguration().groupMembers()).build();
        raftGroup().localLogEntryStore().createEntry(currentTerm(),config);
    }

    private class Replicators {
        private volatile boolean running = true;
        private volatile Thread workingThread;
        private final Map<String, ReplicatorPeer> replicatorPeerMap = new ConcurrentHashMap<>();

        void stop() {
            logger.info("{}: Stop replication thread", groupId());
            replicatorPeerMap.forEach((peer,replicator) -> {
                replicator.stop();
                logger.info("{}: MatchIndex = {}, NextIndex = {}", peer, replicator.matchIndex(), replicator.nextIndex());
            });
            logger.info("{}: last applied: {}", groupId(), raftGroup().logEntryProcessor().lastAppliedIndex());

            running = false;
            notifySenders(null);
            replicatorPeerMap.forEach((nodeId, peer) -> peer.stop());
            workingThread = null;
        }

        void start() {
            workingThread = Thread.currentThread();
            try {

                otherNodesStream().forEach(raftPeer -> {

                    ReplicatorPeer replicatorPeer = new ReplicatorPeer(raftPeer,
                                                                       this::updateMatchIndex,
                                                                       clock,
                                                                       raftGroup(),
                                                                       () -> changeStateTo(stateFactory().followerState()),
                                                                       snapshotManager());
                    replicatorPeer.start();
                    replicatorPeerMap.put(raftPeer.nodeId(), replicatorPeer);
                });

                logger.info("{}: Start replication thread for {} peers", groupId(), replicatorPeerMap.size());

                int parkTime = raftGroup().raftConfiguration().heartbeatTimeout()/2;
                while (running) {
                    int runsWithoutChanges = 0;
                    while (running && runsWithoutChanges < 3) {
                        int sent = 0;
                        for (ReplicatorPeer raftPeer : replicatorPeerMap.values()) {
                            sent += raftPeer.sendNextMessage();
                        }
                        if (sent == 0) {
                            runsWithoutChanges++;
                        } else {
                            LockSupport.parkNanos(100);
                            runsWithoutChanges = 0;
                        }
                    }
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(parkTime));
                }
            } catch (RuntimeException re) {
                logger.warn("Replication thread completed exceptionally", re);
            }
        }



        private void updateMatchIndex(long matchIndex) {
            logger.trace("Updated matchIndex: {}", matchIndex);
            long nextCommitCandidate = raftGroup().logEntryProcessor().commitIndex() + 1;
            boolean updateCommit = false;
            if( matchIndex < nextCommitCandidate) return;
            for( long index = nextCommitCandidate ; index  <= matchIndex && matchedByMajority(index); index++) {
                nextCommitCandidate=index;
                updateCommit=true;
            }

            if( updateCommit &&
                    raftGroup().localLogEntryStore().getEntry(nextCommitCandidate).getTerm() == raftGroup().localElectionStore().currentTerm()) {
                raftGroup().logEntryProcessor().markCommitted(nextCommitCandidate);
            }

        }

        private boolean matchedByMajority(long nextCommitCandidate) {
            int majority = (int) Math.ceil((otherNodesCount() + 1.1) / 2f);
            Stream<Long> matchIndeces = Stream.concat(Stream.of(raftGroup().localLogEntryStore().lastLogIndex()),
                                                      replicatorPeerMap.values().stream().map(peer -> peer.getMatchIndex()));
            return matchIndeces.filter(p -> p >= nextCommitCandidate).count() >= majority;
        }

        void notifySenders(Entry entry) {
            if( workingThread != null)
                LockSupport.unpark(workingThread);

            if( entry != null && otherNodesCount() == 0) {
                raftGroup().logEntryProcessor().markCommitted(entry.getIndex());
            }
        }

        private long lastMessageTimeFromMajority() {
            if( logger.isTraceEnabled()) {
                logger.trace("Last messages received: {}",
                             replicatorPeerMap.values().stream().map(ReplicatorPeer::lastMessageReceived).collect(
                                     Collectors.toList()));
            }
            return replicatorPeerMap.values().stream().map(ReplicatorPeer::lastMessageReceived)
                                    .sorted()
                                    .skip((int)Math.floor(otherNodesCount() / 2f)).findFirst().orElse(0L);
        }

        public void addNode(Node node) {
            if( ! node.getNodeId().equals(me())) {
                RaftPeer raftPeer = raftGroup().peer(node.getNodeId());
                ReplicatorPeer replicatorPeer = new ReplicatorPeer(raftPeer,
                                                                   this::updateMatchIndex,
                                                                   clock,
                                                                   raftGroup(),
                                                                   () -> changeStateTo(stateFactory().followerState()),
                                                                   snapshotManager());
                replicatorPeerMap.put(raftPeer.nodeId(), replicatorPeer);
                replicatorPeer.start();
            }
            appendConfiguration();
        }

        public void removeNode(String nodeId) {
            replicatorPeerMap.remove(nodeId);
            appendConfiguration();
        }
    }
}
