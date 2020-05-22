package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.configuration.ClusterConfiguration;
import io.axoniq.axonserver.cluster.configuration.LeaderConfiguration;
import io.axoniq.axonserver.cluster.configuration.NodeReplicator;
import io.axoniq.axonserver.cluster.exception.ErrorCode;
import io.axoniq.axonserver.cluster.exception.LeadershipTransferInProgressException;
import io.axoniq.axonserver.cluster.exception.LogException;
import io.axoniq.axonserver.cluster.exception.UncommittedTermException;
import io.axoniq.axonserver.cluster.exception.UncommittedConfigException;
import io.axoniq.axonserver.cluster.replication.MatchStrategy;
import io.axoniq.axonserver.cluster.scheduler.Scheduler;
import io.axoniq.axonserver.cluster.util.LeaderTimeoutChecker;
import io.axoniq.axonserver.cluster.util.RoleUtils;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesRequest;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesResponse;
import io.axoniq.axonserver.grpc.cluster.Config;
import io.axoniq.axonserver.grpc.cluster.ConfigChangeResult;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.cluster.LeaderElected;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.cluster.RequestVoteRequest;
import io.axoniq.axonserver.grpc.cluster.RequestVoteResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.FluxSink;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Performs all actions when the node is in leader state.
 *
 * @author Marc Gathier
 * @since 4.1
 */
public class LeaderState extends AbstractMembershipState {

    private static final Logger logger = LoggerFactory.getLogger(LeaderState.class);
    private final ClusterConfiguration clusterConfiguration;

    private final Map<Long, CompletableFuture<Void>> pendingEntries = new ConcurrentHashMap<>();
    private final MatchStrategy matchStrategy;
    private final AtomicLong lastConfirmed = new AtomicLong();
    private final AtomicBoolean leadershipTransferInProgress = new AtomicBoolean();
    private volatile CompletableFuture<ConfigChangeResult> pendingConfigurationChange;
    private volatile Replicators replicators;
    private final LeaderTimeoutChecker leaderTimeoutChecker;

    private LeaderState(Builder builder) {
        super(builder);
        this.matchStrategy = builder.matchStrategy;
        clusterConfiguration = new LeaderConfiguration(raftGroup(),
                                                       () -> currentTimeMillis(),
                                                       this::replicator,
                                                       this::appendConfigurationChange);

        leaderTimeoutChecker = new LeaderTimeoutChecker(this::replicatorPeers,
                                                        maxElectionTimeout(),
                                                        () -> clock(),
                                                        () -> raftGroup().raftConfiguration().minActiveBackups());
    }

    /**
     * Instantiates a new builder for the Leader State.
     *
     * @return a new builder for the Leader State
     */
    public static Builder builder() {
        return new Builder();
    }

    private void appendLeaderElected() {
        logger.info("{} in term {}: Appending info that leader has been elected.", groupId(), currentTerm());
        LeaderElected leaderElected = LeaderElected.newBuilder().setLeaderId(me()).build();
        CompletableFuture<Entry> entry = raftGroup().localLogEntryStore().createEntry(currentTerm(), leaderElected);
        waitCommitted(entry);
    }

    private CompletableFuture<Void> appendConfigurationChange(UnaryOperator<List<Node>> changeOperation) {
        logger.info("{} in term {}: Appending configuration change.", groupId(), currentTerm());
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

        //https://groups.google.com/forum/#!msg/raft-dev/t4xj6dJTP6E/d2D9LrWRza8J
        if (currentTerm() != raftGroup().logEntryProcessor().commitTerm()){
            String message = "No entry has been committed during current term, cannot accept a configuration change.";
            CompletableFuture<Entry> future = new CompletableFuture<>();
            future.completeExceptionally(new UncommittedTermException(message));
            return future;
        }

        Collection<Node> newConfig = configChange.apply(currentConfiguration().groupMembers());
        Config config = Config.newBuilder().addAllNodes(newConfig).build();
        if (config.getNodesCount() == 0) {
            return CompletableFuture.completedFuture(Entry.getDefaultInstance());
        }
        return raftGroup().localLogEntryStore().createEntry(currentTerm(), config);
    }

    private NodeReplicator replicator(Node node) {
        return matchIndexUpdates -> replicators.addNonVotingNode(node, matchIndexUpdates);
    }

    @Override
    public CompletableFuture<ConfigChangeResult> addServer(Node node) {
        pendingConfigurationChange = clusterConfiguration.addServer(node);
        return pendingConfigurationChange;
    }

    @Override
    public CompletableFuture<ConfigChangeResult> removeServer(String nodeId) {
        pendingConfigurationChange = clusterConfiguration.removeServer(nodeId);
        return pendingConfigurationChange;
    }

    @Override
    public void start() {
        super.start();
        leadershipTransferInProgress.set(false);
        replicators = new Replicators();
        lastConfirmed.set(0);
        replicators.start();
        //It is important to have leaderElected log entry appended only after the replicators are started,
        //because when there is only one node into the group, the replicators are used to directly update commit index.
        appendLeaderElected();
        scheduleStepDownTimeoutChecker();
    }

    @Override
    public void stop() {
        super.stop();
        if (replicators != null) {
            replicators.stop();
            replicators = null;
        }
        if (pendingConfigurationChange != null && !pendingConfigurationChange.isDone()) {
            pendingConfigurationChange.completeExceptionally(new IllegalStateException(
                    "Leader stepped down during processing of transaction"));
            pendingConfigurationChange = null;
        }
        pendingEntries.forEach((index, completableFuture) -> completableFuture
                .completeExceptionally(new LeadershipTransferInProgressException("Transferring leadership")));
        pendingEntries.clear();
        logger.info("{} in term {}: {} steps down from Leader role.", groupId(), currentTerm(), me());
    }

    @Override
    public AppendEntriesResponse appendEntries(AppendEntriesRequest request) {
        if (request.getTerm() >= currentTerm()) {
            logger.info("{} in term {}: Append Entries from leader {}: Received term {} which is greater or equals than mine. Moving to Follower...",
                        groupId(),
                        currentTerm(),
                        request.getLeaderId(),
                        request.getTerm());
            String message = format("%s received AppendEntriesRequest with greater or equals term (%s >= %s) from %s",
                                    me(), request.getTerm(), currentTerm(), request.getLeaderId());
            return handleAsFollower(follower -> follower.appendEntries(request), message);
        }
        logger.info("{} in term {}: Append Entries from leader {}: Received term {} is smaller than mine. Rejecting the request.",
                    groupId(),
                    currentTerm(),
                    request.getLeaderId(),
                    request.getTerm());
        return responseFactory().appendEntriesFailure(request.getRequestId(), "Request rejected because I'm a leader");
    }


    @Override
    public RequestVoteResponse requestVote(RequestVoteRequest request) {
        if (!request.getDisruptAllowed() && heardFromFollowers()) {
            return responseFactory().voteRejected(request.getRequestId());
        }

        return super.requestVote(request);
    }

    @Override
    public RequestVoteResponse requestPreVote(RequestVoteRequest request) {
        if (heardFromFollowers()) {
            return responseFactory().voteRejected(request.getRequestId());
        }

        return super.requestPreVote(request);
    }


    @Override
    protected boolean shouldGoAwayIfNotMember() {
        return true;
    }

    @Override
    public boolean isLeader() {
        return true;
    }

    @Override
    public CompletableFuture<Void> appendEntry(String entryType, byte[] entryData) {
        if( leadershipTransferInProgress.get()) {
            throw new LeadershipTransferInProgressException("Transferring leadership");
        }
        return createEntry(currentTerm(), entryType, entryData);
    }

    private void scheduleStepDownTimeoutChecker() {
        schedule(s -> s.schedule(this::checkStepdown, maxElectionTimeout(), MILLISECONDS));
    }

    @Override
    public void forceStartElection() {
        String message = format("%s in term %s: Forced Step Down of %s.", groupId(), currentTerm(), me());
        logger.info(message);
        stepDown(message);
    }

    private void stepDown(String cause) {
        changeStateTo(stateFactory().followerState(), cause);
    }

    private boolean heardFromFollowers() {
        return leaderTimeoutChecker.heardFromFollowers().result();
    }

    private void checkStepdown() {
        LeaderTimeoutChecker.CheckResult checkResult = leaderTimeoutChecker.heardFromFollowers();
        if (!checkResult.result()) {
            String message = format("%s in term %s: StepDown as %s",
                                    groupId(),
                                    currentTerm(),
                                    checkResult.reason());
            logger.info(message);
            changeStateTo(stateFactory().followerState(), message);
        } else {
            logger.trace("{} in term {}: Reschedule checkStepdown after {}ms",
                         groupId(),
                         currentTerm(),
                         checkResult.nextCheckInterval());
            schedule(s -> s.schedule(this::checkStepdown, checkResult.nextCheckInterval(), MILLISECONDS));
        }
    }

    /**
     * Stops accepting new requests from client and waits until one of the followers is up to date. When
     * a follower is up to date, if sends a timeout now message to the follower to force a new election.
     * @return completable future that completes when a condidate leader is updated
     */
    @Override
    public CompletableFuture<Void> transferLeadership() {
        if (otherNodesCount() == 0) {
            throw new LogException(ErrorCode.VALIDATION_FAILED,
                                   "Cannot transfer leadership if no other nodes avaiable");
        }

        if (leadershipTransferInProgress.compareAndSet(false, true)) {
            CompletableFuture<Void> completableFuture = new CompletableFuture<>();
            waitForFollowerUpdated(completableFuture);
            return completableFuture;
        }
        throw new LeadershipTransferInProgressException("Transfer leadership already in progress");
    }

    private void waitForFollowerUpdated(CompletableFuture<Void> completableFuture) {
        Optional<ReplicatorPeer> updatedFollower =
                findUpToDatePeer();

        if (updatedFollower.isPresent()) {
            updatedFollower.get().sendTimeoutNow();
            completableFuture.complete(null);
        } else {
            schedule(s -> s.schedule(() -> waitForFollowerUpdated(completableFuture), 10, TimeUnit.MILLISECONDS));
        }
    }

    /**
     * Finds a (voting) peer that has confirmed to have received all log entries from the leader.
     *
     * @return optional replicator peer
     */
    private Optional<ReplicatorPeer> findUpToDatePeer() {
        long lastLogEntry = raftGroup().localLogEntryStore().lastLogIndex();
        return replicators.replicatorPeerMap
                .entrySet()
                .stream()
                .filter(e -> replicators.isPossibleLeader(e.getValue()))
                .filter(e -> e.getValue().nextIndex() > lastLogEntry)
                .map(Map.Entry::getValue)
                .findFirst();
    }

    private CompletableFuture<Void> createEntry(long currentTerm, String entryType, byte[] entryData) {
        CompletableFuture<Entry> entryFuture = raftGroup().localLogEntryStore()
                                                          .createEntry(currentTerm, entryType, entryData);
        return waitCommitted(entryFuture);
    }

    private CompletableFuture<Void> waitCommitted(CompletableFuture<Entry> entryFuture) {
        CompletableFuture<Void> appendEntryDone = new CompletableFuture<>();
        entryFuture.whenComplete((e, failure) -> {
            if (failure != null) {
                logger.warn("{} in term {}: Storing entry failed", groupId(), currentTerm(), failure);
                appendEntryDone.completeExceptionally(failure);
            } else {
                try {
                    if (replicators == null) {
                        appendEntryDone.completeExceptionally(new LogException(ErrorCode.CLUSTER_ERROR,
                                                                               "Replicators null when processing entry in LeaderState"));
                    } else {
                        pendingEntries.put(e.getIndex(), appendEntryDone);
                        replicators.updateCommitIndex(e.getIndex());
                        replicators.notifySenders();
                    }
                } catch (Exception ex) {
                    logger.info("{} in term {}: problem happened during replicators update.", groupId(), currentTerm(), ex);
                    appendEntryDone.completeExceptionally(ex);
                }
            }
        });
        return appendEntryDone;
    }

    @Override
    public void applied(Entry e) {
        try {
            CompletableFuture<Void> completableFuture = pendingEntries.remove(e.getIndex());
            int retries = 5;
            while (completableFuture == null && lastConfirmed.get() < e.getIndex() && retries-- > 0) {
                Thread.sleep(1);
                completableFuture = pendingEntries.remove(e.getIndex());
            }
            if (completableFuture != null) {
                completableFuture.complete(null);
                lastConfirmed.set(e.getIndex());
            } else {
                logger.debug(
                        "Failed to retrieve waiting call for log entry index {}, could not complete the request.",
                        e.getIndex());
            }
        } catch (InterruptedException e1) {
            Thread.currentThread().interrupt();
            logger.debug("interrupted in apply");
        }
    }

    @Override
    public String getLeader() {
        return me();
    }

    @Override
    public Iterator<ReplicatorPeerStatus> replicatorPeers() {
        if (replicators == null) {
            return Collections.emptyIterator();
        }
        return replicators.replicatorPeerMap.values()
                                            .stream()
                                            .filter(peer -> !replicators.nonVotingReplicaMap
                                                    .containsKey(peer.nodeName()))
                                            .map(peer -> (ReplicatorPeerStatus) peer)
                                            .iterator();
    }

    @Override
    protected void updateCurrentTerm(long term, String cause) {
        if (term <= raftGroup().localElectionStore().currentTerm()) {
            return;
        }
        stepDown(format("Stepping down because of greater term %s. My term %s", term, currentTerm()));
        super.updateCurrentTerm(term, cause);
    }

    /**
     * A Builder for {@link LeaderState}.
     *
     * @author Marc Gathier
     * @since 4.1
     */
    protected static class Builder extends AbstractMembershipState.Builder<Builder> {

        private MatchStrategy matchStrategy;

        public LeaderState build() {
            return new LeaderState(this);
        }

        public LeaderState.Builder matchStrategy(MatchStrategy matchStrategy) {
            this.matchStrategy = matchStrategy;
            return this;
        }
    }

    /**
     * Checks health of the node if it is in leader state.
     *
     * @param statusConsumer consumer to provide status messages to
     * @return true if this node considers itself healthy
     */
    @Override
    public boolean health(BiConsumer<String, String> statusConsumer) {
        long lastLogIndex = lastLogIndex();
        long now = System.currentTimeMillis();
        AtomicBoolean result = new AtomicBoolean(super.health(statusConsumer));
        replicatorPeers().forEachRemaining(
                rp -> {
                    long lastMessageAge = now - rp.lastMessageReceived();
                    long raftMsgBuffer = lastLogIndex - (rp.nextIndex() - 1);
                    if (lastMessageAge > maxElectionTimeout()) {
                        statusConsumer.accept(groupId() + ".follower." + rp.nodeName() + ".status", "NO_ACK_RECEIVED");
                        result.set(false);
                    } else if (raftMsgBuffer > 100) {
                        statusConsumer.accept(groupId() + ".follower." + rp.nodeName() + ".status", "BEHIND");
                        result.set(false);
                    } else {
                        statusConsumer.accept(groupId() + ".follower." + rp.nodeName() + ".status", "UP");
                    }
                }
        );
        return result.get();
    }

    private class Replicators {

        private final Map<String, Disposable> nonVotingReplicaMap = new ConcurrentHashMap<>();
        private final List<Registration> registrations = new ArrayList<>();
        private final Map<String, ReplicatorPeer> replicatorPeerMap = new ConcurrentHashMap<>();
        private final AtomicBoolean replicationRunning = new AtomicBoolean(false);

        void stop() {
            logger.info("{} in term {}: Stop replication thread", groupId(), currentTerm());
            long now = currentTimeMillis();
            replicatorPeerMap.forEach((peer, replicator) -> {
                replicator.stop();
                logger.info(
                        "{} in term {}: {}: MatchIndex = {}, NextIndex = {}, lastMessageReceived = {}ms ago, lastMessageSent = {}ms ago",
                        groupId(),
                        currentTerm(),
                        peer,
                        replicator.matchIndex(),
                        replicator.nextIndex(),
                        now - replicator.lastMessageReceived(),
                        now - replicator.lastMessageSent());
            });
            logger.info("{} in term {}: last applied: {}",
                        groupId(),
                        currentTerm(),
                        raftGroup().logEntryProcessor().lastAppliedIndex());

            nonVotingReplicaMap.forEach((nodeId, replication) -> replication.dispose());
            notifySenders();
            registrations.forEach(Registration::cancel);
        }

        private void start() {
            registrations.add(registerConfigurationListener(this::updateNodes));
            try {
                otherPeersStream().forEach(this::registerNode);
                execute(this::replicate);
                logger.info("{} in term {}: Start replication thread for {} peers.",
                            groupId(),
                            currentTerm(),
                            replicatorPeerMap.size());
            } catch (RuntimeException re) {
                logger.warn("{} in term {}: Start of LeaderState failed.", groupId(), currentTerm(), re);
                throw re;
            }
        }

        private void replicate() {
            if (!replicationRunning.compareAndSet(false, true)) {
                // it's fine, replication is already in progress
                return;
            }
            try {
                int runsWithoutChanges = 0;
                while (runsWithoutChanges < 3) {
                    int sent = 0;
                    for (ReplicatorPeer raftPeer : replicatorPeerMap.values()) {
                        sent += time(raftPeer::sendNextMessage, raftPeer.nodeId());
                    }
                    if (sent == 0) {
                        runsWithoutChanges++;
                    } else {
                        runsWithoutChanges = 0;
                    }
                }
            } finally {
                schedule(s -> s.schedule(this::replicate,
                                         raftGroup().raftConfiguration().heartbeatTimeout() / 2,
                                         MILLISECONDS));
                replicationRunning.set(false);
            }
        }

        private int time(Supplier<Integer> operation, String target) {
            long before = currentTimeMillis();
            try {
                return operation.get();
            } finally {
                if( logger.isTraceEnabled()) {
                    long after = currentTimeMillis();
                    if (after - raftGroup().raftConfiguration().heartbeatTimeout() > before) {
                        logger.trace("{}: Action took {}ms", target, after - before);
                    }
                }
            }
        }

        private void updateCommitIndex(long matchIndex) {
            logger.trace("{} in term {}: Updated matchIndex: {}.", groupId(), currentTerm(), matchIndex);
            long nextCommitCandidate = raftGroup().logEntryProcessor().commitIndex() + 1;
            boolean updateCommit = false;
            if (matchIndex < nextCommitCandidate) {
                return;
            }
            for (long index = nextCommitCandidate; index <= matchIndex && matchStrategy.match(index); index++) {
                nextCommitCandidate = index;
                updateCommit = true;
            }

            Entry entry = raftGroup().localLogEntryStore().getEntry(nextCommitCandidate);
            if (updateCommit && entry.getTerm() == raftGroup().localElectionStore().currentTerm()) {
                raftGroup().logEntryProcessor().markCommitted(entry.getIndex(), entry.getTerm());
            }
        }

        void notifySenders() {
            if (!replicationRunning.get()) {
                execute(this::replicate);
            }
        }

        public void updateNodes(List<Node> nodes) {
            Set<String> toRemove = new HashSet<>(replicatorPeerMap.keySet());
            for (Node node : nodes) {
                toRemove.remove(node.getNodeName());
                if (!replicatorPeerMap.containsKey(node.getNodeName())
                        && !node.getNodeId().equals(me())) {
                    registerNode(raftGroup().peer(node));
                }
            }
            toRemove.removeAll(nonVotingReplicaMap.keySet());
            toRemove.forEach(this::removeNode);
        }

        private void registerNode(RaftPeer raftPeer) {
            EmitterProcessor<Long> processor = EmitterProcessor.create(10);
            processor.replay().autoConnect().subscribe(this::updateCommitIndex);
            registrations.add(registerPeer(raftPeer, processor.sink()));
        }

        public void removeNode(String nodeName) {
            ReplicatorPeer removed = replicatorPeerMap.remove(nodeName);
            if (removed != null) {
                removed.stop();
            }
        }

        private Disposable addNonVotingNode(Node node, FluxSink<Long> matchIndexUpdates) {
            String nodeName = node.getNodeName();
            if (replicatorPeerMap.containsKey(nodeName)) {
                throw new IllegalArgumentException("Replicators already contain the node " + node.getNodeName());
            }
            Registration registration = registerPeer(raftGroup().peer(node), matchIndexUpdates);
            Disposable replication = () -> {
                registration.cancel();
                nonVotingReplicaMap.remove(nodeName);
            };
            nonVotingReplicaMap.put(nodeName, replication);
            return replication;
        }

        private Registration registerPeer(RaftPeer raftPeer, FluxSink<Long> matchIndexUpdates) {
            ReplicatorPeer replicatorPeer = new ReplicatorPeer(raftPeer,
                                                               matchIndexUpdates,
                                                               clock(),
                                                               raftGroup(),
                                                               snapshotManager(),
                                                               LeaderState.this::updateCurrentTerm,
                                                               LeaderState.this::lastLogIndex);
            replicatorPeer.start();
            replicatorPeerMap.put(raftPeer.nodeName(), replicatorPeer);
            return () -> {
                ReplicatorPeer removed = replicatorPeerMap.remove(raftPeer.nodeName());
                if (removed != null) {
                    removed.stop();
                }
                matchIndexUpdates.complete();
            };
        }

        /**
         * Checks if this peer is a node capable of being a leader.
         *
         * @param peer the raft peer
         * @return true if peer is able to become leader
         */
        public boolean isPossibleLeader(ReplicatorPeer peer) {
            return !nonVotingReplicaMap.containsKey(peer.nodeId()) && RoleUtils.primaryNode(peer.role());
        }
    }
}
