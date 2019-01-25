package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.configuration.ClusterConfiguration;
import io.axoniq.axonserver.cluster.configuration.LeaderConfiguration;
import io.axoniq.axonserver.cluster.configuration.NodeReplicator;
import io.axoniq.axonserver.cluster.exception.UncommittedConfigException;
import io.axoniq.axonserver.cluster.scheduler.Scheduler;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesRequest;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesResponse;
import io.axoniq.axonserver.grpc.cluster.Config;
import io.axoniq.axonserver.grpc.cluster.ConfigChangeResult;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotRequest;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotResponse;
import io.axoniq.axonserver.grpc.cluster.LeaderElected;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.cluster.RequestVoteRequest;
import io.axoniq.axonserver.grpc.cluster.RequestVoteResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
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
    private final AtomicReference<Scheduler> scheduler = new AtomicReference<>();

    //private final NavigableMap<Long, CompletableFuture<Void>> pendingEntries = new ConcurrentSkipListMap<>();
    private final Map<Long, CompletableFuture<Void>> pendingEntries = new ConcurrentHashMap<>();
    private volatile Replicators replicators;

    protected static class Builder extends AbstractMembershipState.Builder<Builder> {

        public LeaderState build() {
            return new LeaderState(this);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    private LeaderState(Builder builder) {
        super(builder);
        clusterConfiguration = new LeaderConfiguration(raftGroup(),
                                                       () -> scheduler.get().clock().millis(),
                                                       this::replicator,
                                                       this::appendConfigurationChange);
    }


    private CompletableFuture<Void> appendLeaderElected(){
        LeaderElected leaderElected = LeaderElected.newBuilder().setLeaderId(me()).build();
        CompletableFuture<Entry> entry = raftGroup().localLogEntryStore().createEntry(currentTerm(), leaderElected);
        return waitCommitted(entry);
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
        appendLeaderElected();
        scheduler.set(schedulerFactory().get());
        scheduleStepDownTimeoutChecker();
        replicators = new Replicators();
        replicators.start();
    }

    @Override
    public void stop() {
        replicators.stop();
        replicators = null;
        pendingEntries.forEach((index, completableFuture) -> completableFuture
                .completeExceptionally(new IllegalStateException("Leader stepped down during processing of transaction")));
        pendingEntries.clear();
        if (scheduler.get() != null) {
            scheduler.getAndSet(null).shutdownNow();
        }
    }

    @Override
    public AppendEntriesResponse appendEntries(AppendEntriesRequest request) {
        logger.trace("{}: Received appendEntries request. Rejecting the request.", groupId());
        return appendEntriesFailure(request.getRequestId());
    }

    @Override
    public RequestVoteResponse requestVote(RequestVoteRequest request) {
        logger.warn("{}: Request for vote received from {} in term {}. Rejecting the request", groupId(),
                    request.getCandidateId(), request.getTerm());
        return requestVoteResponse(request.getRequestId(),false);
    }

    @Override
    public InstallSnapshotResponse installSnapshot(InstallSnapshotRequest request) {
        logger.trace("{}: Received installSnapshot request. Rejecting the request.", groupId());
        return installSnapshotFailure(request.getRequestId());
    }

    @Override
    public boolean isLeader() {
        return true;
    }

    @Override
    public CompletableFuture<Void> appendEntry(String entryType, byte[] entryData) {
        return createEntry(currentTerm(), entryType, entryData);
    }

    private void scheduleStepDownTimeoutChecker() {
        scheduler.get().schedule(this::checkStepdown, maxElectionTimeout(), MILLISECONDS);
    }

    public void forceStepDown() {
        logger.info("{}: StepDown forced", groupId());
        changeStateTo(stateFactory().followerState());
    }

    private void checkStepdown() {
        if (otherNodesCount() == 0) {
            return;
        }
        long now = scheduler.get().clock().millis();
        long lastReceived = replicators.lastMessageTimeFromMajority();
        if (now - lastReceived > maxElectionTimeout()) {
            logger.info("{}: StepDown as no messages received for {}ms", groupId(), (now - lastReceived));
            changeStateTo(stateFactory().followerState());
        } else {
            logger.trace("{}: Reschedule checkStepdown after {}ms",
                         groupId(),
                         maxElectionTimeout() - (now - lastReceived));
            scheduler.get().schedule(this::checkStepdown, maxElectionTimeout() - (now - lastReceived), MILLISECONDS);
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
//        if (replicators == null) {
//            appendEntryDone.completeExceptionally(new RuntimeException("Step down in progress"));
//            return appendEntryDone;
//        }
        entryFuture.whenComplete((e, failure) -> {
            if (failure != null) {
                logger.warn("Storing entry failed", failure);
                appendEntryDone.completeExceptionally(failure);
            } else {
                pendingEntries.put(e.getIndex(), appendEntryDone);
                if (replicators != null) {
                    replicators.notifySenders();
                }
                replicators.updateMatchIndex(e.getIndex());
            }
        });
        return appendEntryDone;
    }

    @Override
    public void applied(Entry e) {
        try {
            CompletableFuture<Void> completableFuture = pendingEntries.remove(e.getIndex());
            int retries = 5;
            while( completableFuture == null && retries-- > 0) {
                logger.info("waiting for {}", e.getIndex());
                Thread.sleep(1);
                completableFuture = pendingEntries.remove(e.getIndex());
            }
            if( completableFuture != null) {
                completableFuture.complete(null);
            }
        } catch (InterruptedException e1) {
            Thread.currentThread().interrupt();
            logger.debug("interrupted in apply");
        }
//        Map.Entry<Long, CompletableFuture<Void>> first = pendingEntries.pollFirstEntry();
//        boolean found = false;
//        while (first != null && first.getKey() <= e.getIndex()) {
//            first.getValue().complete(null);
//            found = e.getIndex() == first.getKey();
//            first = pendingEntries.pollFirstEntry();
//        }
//
//        if (!found && logger.isInfoEnabled()) {
//            logger.info("{}: entry not found when applied {} - {}", groupId(), e.getIndex(), pendingEntries.keySet());
//        }
//        if (first != null) {
//            pendingEntries.put(first.getKey(), first.getValue());
//        }
    }

    @Override
    public String getLeader() {
        return me();
    }

    private class Replicators {

        private final Set<String> nonVotingReplica = new CopyOnWriteArraySet<>();
        private final List<Registration> registrations = new ArrayList<>();
        private final Map<String, ReplicatorPeer> replicatorPeerMap = new ConcurrentHashMap<>();
        private final AtomicBoolean replicationRunning = new AtomicBoolean(false);

        void stop() {
            logger.info("{}: Stop replication thread", groupId());
            long now = scheduler.get().clock().millis();
            replicatorPeerMap.forEach((peer, replicator) -> {
                replicator.stop();
                logger.info(
                        "{}: MatchIndex = {}, NextIndex = {}, lastMessageReceived = {}ms ago, lastMessageSent = {}ms ago",
                        peer,
                        replicator.matchIndex(),
                        replicator.nextIndex(),
                        now - replicator.lastMessageReceived(),
                        now - replicator.lastMessageSent());
            });
            logger.info("{}: last applied: {}", groupId(), raftGroup().logEntryProcessor().lastAppliedIndex());

            notifySenders();
            registrations.forEach(Registration::cancel);
        }

        private void start() {
            registrations.add(registerConfigurationListener(this::updateNodes));
            try {
                otherPeersStream().forEach(peer -> registrations.add(registerPeer(peer, this::updateMatchIndex)));
                replicate();
                logger.info("{}: Start replication thread for {} peers", groupId(), replicatorPeerMap.size());

            } catch (RuntimeException re) {
                logger.warn("Replication thread completed exceptionally", re);
            }
        }

        private void replicate() {
            if (!replicationRunning.compareAndSet(false, true)) {
                // it's fine, replication is already in progress
                return;
            }
            Optional.ofNullable(scheduler.get())
                    .ifPresent(schedulerInstance -> {
                        try {
                            int runsWithoutChanges = 0;
                            while (runsWithoutChanges < 3) {
                                int sent = 0;
                                for (ReplicatorPeer raftPeer : replicatorPeerMap.values()) {
                                    sent += raftPeer.sendNextMessage();
                                }
                                if (sent == 0) {
                                    runsWithoutChanges++;
                                } else {
                                    runsWithoutChanges = 0;
                                }
                            }
                        } finally {
                            schedulerInstance.schedule(this::replicate,
                                                       raftGroup().raftConfiguration().heartbeatTimeout(),
                                                       MILLISECONDS);
                            replicationRunning.set(false);
                        }
                    });
        }

        private void updateMatchIndex(long matchIndex) {
            logger.trace("Updated matchIndex: {}", matchIndex);
            long nextCommitCandidate = raftGroup().logEntryProcessor().commitIndex() + 1;
            boolean updateCommit = false;
            if (matchIndex < nextCommitCandidate) {
                return;
            }
            for (long index = nextCommitCandidate; index <= matchIndex && matchedByMajority(index); index++) {
                nextCommitCandidate = index;
                updateCommit = true;
            }

            Entry entry = raftGroup().localLogEntryStore().getEntry(nextCommitCandidate);
            if (updateCommit && entry.getTerm() == raftGroup().localElectionStore().currentTerm()) {
                raftGroup().logEntryProcessor().markCommitted(entry.getIndex(), entry.getTerm());
            }
        }

        private boolean matchedByMajority(long nextCommitCandidate) {
            int majority = (int) Math.ceil((otherNodesCount() + 1.1) / 2f);
            Stream<Long> matchIndices = Stream.concat(Stream.of(raftGroup().localLogEntryStore().lastLogIndex()),
                                                      replicatorPeerMap.values().stream()
                                                                       .map(ReplicatorPeer::getMatchIndex));
            return matchIndices.filter(p -> p >= nextCommitCandidate).count() >= majority;
        }

        void notifySenders() {
            replicate();
        }

        private long lastMessageTimeFromMajority() {
            if (logger.isTraceEnabled()) {
                logger.trace("Last messages received: {}",
                             replicatorPeerMap.values().stream().map(ReplicatorPeer::lastMessageReceived).collect(
                                     Collectors.toList()));
            }
            return replicatorPeerMap.values().stream().map(ReplicatorPeer::lastMessageReceived)
                                    .sorted()
                                    .skip((int) Math.floor(otherNodesCount() / 2f)).findFirst().orElse(0L);
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
            if (!node.getNodeId().equals(me())) {
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
                                                               scheduler.get().clock(),
                                                               raftGroup(),
                                                               snapshotManager());
            replicatorPeer.start();
            replicatorPeerMap.put(raftPeer.nodeId(), replicatorPeer);
            return () -> {
                ReplicatorPeer removed = replicatorPeerMap.remove(raftPeer.nodeId());
                if (removed != null) {
                    removed.stop();
                }
            };
        }
    }
}
