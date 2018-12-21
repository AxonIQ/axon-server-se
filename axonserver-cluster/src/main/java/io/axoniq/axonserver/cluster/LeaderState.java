package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.Scheduler.ScheduledRegistration;
import io.axoniq.axonserver.cluster.configuration.ClusterConfiguration;
import io.axoniq.axonserver.cluster.configuration.LeaderConfiguration;
import io.axoniq.axonserver.cluster.configuration.NodeReplicator;
import io.axoniq.axonserver.cluster.exception.UncommittedConfigException;
import io.axoniq.axonserver.cluster.util.AxonThreadFactory;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesRequest;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesResponse;
import io.axoniq.axonserver.grpc.cluster.Config;
import io.axoniq.axonserver.grpc.cluster.ConfigChangeResult;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotRequest;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotResponse;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.cluster.RequestVoteRequest;
import io.axoniq.axonserver.grpc.cluster.RequestVoteResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Author: marc
 */
public class LeaderState extends AbstractMembershipState {

    private final ClusterConfiguration clusterConfiguration;

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

    public static Builder builder() {
        return new Builder();
    }

    private LeaderState(Builder builder) {
        super(builder);
        executor = Executors.newCachedThreadPool(new AxonThreadFactory("Replicator-" + groupId()));
        clock = scheduler().clock();
        clusterConfiguration = new LeaderConfiguration(raftGroup(),
                                                       clock::millis,
                                                       this::replicator,
                                                       this::appendConfigurationChange);
    }

    private CompletableFuture<Void> appendConfigurationChange(UnaryOperator<List<Node>> changeOperation) {
        CompletableFuture<Entry> entry = accept(changeOperation);
        return waitCommitted(entry);
    }

    protected synchronized CompletableFuture<Entry> accept(UnaryOperator<List<Node>> configChange) {
        if (currentConfiguration().isUncommitted()) {
            String message = "Exists an uncommitted configuration, cannot accept a new change.";
            CompletableFuture<Entry> future = new CompletableFuture<>();
            future.completeExceptionally(new UncommittedConfigException(message));
            return future;
        }
        Collection<Node> newConfig = configChange.apply(currentConfiguration().groupMembers());
        Config config = Config.newBuilder().addAllNodes(newConfig).build();
        return raftGroup().localLogEntryStore().createEntry(currentTerm(), config);
    }

    private NodeReplicator replicator(Node node) {
        return matchIndexCallback -> replicators.addNonVotingNode(node, matchIndexCallback);
    }

    @Override
    public CompletableFuture<ConfigChangeResult> addServer(Node node) {
        return clusterConfiguration.addServer(node);
    }

    @Override
    public CompletableFuture<ConfigChangeResult> removeServer(String nodeId) {
        return clusterConfiguration.removeServer(nodeId);
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
        pendingEntries.forEach((index, completableFuture) -> completableFuture
                .completeExceptionally(new IllegalStateException()));
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

    private void cancelStepDown() {
        stepDown.get().cancel();
    }

    public void forceStepDown() {
        logger.info("{}: StepDown forced", groupId());
        changeStateTo(stateFactory().followerState());
    }

    private void checkStepdown() {
        if (otherPeers().isEmpty()) {
            return;
        }
        long now = clock.millis();
        long lastReceived = replicators.lastMessageTimeFromMajority();
        if( now - lastReceived > maxElectionTimeout()) {
            logger.info("{}: StepDown as no messages received for {}ms", groupId(), (now-lastReceived));
            changeStateTo(stateFactory().followerState());
        } else {
            logger.trace("{}: Reschedule checkStepdown after {}ms",
                         groupId(),
                         maxElectionTimeout() - (now - lastReceived));
            ScheduledRegistration newTask = scheduler().schedule(this::checkStepdown,
                                                                 maxElectionTimeout() - (now - lastReceived),
                                                                 MILLISECONDS);
            stepDown.set(newTask);
        }
    }

    private CompletableFuture<Void> createEntry(long currentTerm, String entryType, byte[] entryData) {
        CompletableFuture<Entry> entryFuture = raftGroup().localLogEntryStore().createEntry(currentTerm,
                                                                                            entryType,
                                                                                            entryData);
        return waitCommitted(entryFuture);
    }

    private CompletableFuture<Void> waitCommitted(CompletableFuture<Entry> entryFuture) {
        CompletableFuture<Void> appendEntryDone = new CompletableFuture<>();
        if( replicators == null) {
            appendEntryDone.completeExceptionally(new RuntimeException("Step down in progress"));
            return appendEntryDone;
        }
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

    private class Replicators {

        private volatile boolean running = true;
        private volatile Thread workingThread;
        private final Set<String> nonVotingReplica = new CopyOnWriteArraySet<>();
        private final List<Registration> registrations = new ArrayList<>();
        private final Map<String, ReplicatorPeer> replicatorPeerMap = new ConcurrentHashMap<>();

        void stop() {
            logger.info("{}: Stop replication thread", groupId());
            long now = clock.millis();
            replicatorPeerMap.forEach((peer, replicator) -> {
                replicator.stop();
                logger.info("{}: MatchIndex = {}, NextIndex = {}, lastMessageReceived = {}ms ago, lastMessageSent = {}ms ago",
                            peer,
                            replicator.matchIndex(),
                            replicator.nextIndex(),
                            now - replicator.lastMessageReceived(), now - replicator.lastMessageSent());
            });
            logger.info("{}: last applied: {}", groupId(), raftGroup().logEntryProcessor().lastAppliedIndex());

            running = false;
            notifySenders(null);
            registrations.forEach(Registration::cancel);
            workingThread = null;
        }

        void start() {
            registrations.add(registerConfigurationListener(this::updateNodes));
            workingThread = Thread.currentThread();
            try {

                otherPeersStream().forEach(peer -> registrations.add(registerPeer(peer, this::updateMatchIndex)));

                logger.info("{}: Start replication thread for {} peers", groupId(), replicatorPeerMap.size());

                int parkTime = raftGroup().raftConfiguration().heartbeatTimeout() / 2;
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

        public void updateNodes(List<Node> nodes) {
            Set<String> toRemove = new HashSet<>(replicatorPeerMap.keySet());
            for (Node node : nodes) {
                toRemove.remove(node.getNodeId());
                if (!replicatorPeerMap.containsKey(node.getNodeId()) && !node.getNodeId().equals(me())) {
                    addNode(node);
                }
            }
            toRemove.removeAll(nonVotingReplica);
            toRemove.forEach(this::removeNode);
        }

        public void addNode(Node node) {
            if( ! node.getNodeId().equals(me())) {
                RaftPeer raftPeer = raftGroup().peer(node);
                registrations.add(registerPeer(raftPeer, this::updateMatchIndex));
            }
        }

        public void removeNode(String nodeId) {
            ReplicatorPeer removed = replicatorPeerMap.remove(nodeId);
            if (removed != null) {
                removed.stop();
            }
        }

        Disposable addNonVotingNode(Node node, Consumer<Long> matchIndexCallback) {
            if (replicatorPeerMap.containsKey(node.getNodeId())) {
                throw new IllegalArgumentException("Replicators already contain the node " + node.getNodeId());
            }
            Registration registration = registerPeer(raftGroup().peer(node), matchIndexCallback);
            nonVotingReplica.add(node.getNodeId());
            return () -> {
                registration.cancel();
                nonVotingReplica.remove(node.getNodeId());
            };
        }

        private Registration registerPeer(RaftPeer raftPeer, Consumer<Long> matchIndexCallback) {
            ReplicatorPeer replicatorPeer = new ReplicatorPeer(raftPeer,
                                                               matchIndexCallback,
                                                               clock,
                                                               raftGroup(),
                                                               () -> changeStateTo(stateFactory().followerState()),
                                                               snapshotManager());
            replicatorPeerMap.put(raftPeer.nodeId(), replicatorPeer);
            replicatorPeer.start();
            return () -> {
                ReplicatorPeer removed = replicatorPeerMap.remove(raftPeer.nodeId());
                if (removed != null) {
                    removed.stop();
                }
            };
        }
    }
}
