package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.configuration.ClusterConfiguration;
import io.axoniq.axonserver.cluster.configuration.LeaderConfiguration;
import io.axoniq.axonserver.cluster.configuration.NodeReplicator;
import io.axoniq.axonserver.cluster.exception.UncommittedConfigException;
import io.axoniq.axonserver.cluster.replication.MatchStrategy;
import io.axoniq.axonserver.cluster.scheduler.Scheduler;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesRequest;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesResponse;
import io.axoniq.axonserver.grpc.cluster.Config;
import io.axoniq.axonserver.grpc.cluster.ConfigChangeResult;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.cluster.LeaderElected;
import io.axoniq.axonserver.grpc.cluster.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

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
    private final AtomicReference<Scheduler> scheduler = new AtomicReference<>();

    private final Map<Long, CompletableFuture<Void>> pendingEntries = new ConcurrentHashMap<>();
    private final MatchStrategy matchStrategy;
    private final AtomicLong lastConfirmed = new AtomicLong();
    private volatile Replicators replicators;

    private LeaderState(Builder builder) {
        super(builder);
        this.matchStrategy = builder.matchStrategy;
        clusterConfiguration = new LeaderConfiguration(raftGroup(),
                                                       () -> scheduler.get().clock().millis(),
                                                       this::replicator,
                                                       this::appendConfigurationChange);
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
        Collection<Node> newConfig = configChange.apply(currentConfiguration().groupMembers());
        Config config = Config.newBuilder().addAllNodes(newConfig).build();
        if (config.getNodesCount() == 0) {
            return CompletableFuture.completedFuture(Entry.getDefaultInstance());
        }
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
        return clusterConfiguration.removeServer(nodeId).thenApply(configChangeResult -> checkCurrentNodeDeleted(
                configChangeResult, nodeId));
    }

    private ConfigChangeResult checkCurrentNodeDeleted(ConfigChangeResult configChangeResult, String nodeId) {
        if (nodeId.equals(me())) {
            logger.warn("{} in term {}: Check Current leader deleted: {}", groupId(), currentTerm(), nodeId);
            changeStateTo(stateFactory().removedState(), "Node deleted from group");
        }
        return configChangeResult;
    }

    @Override
    public void start() {
        replicators = new Replicators();
        scheduler.set(schedulerFactory().get());
        lastConfirmed.set(0);
        replicators.start();
        //It is important to have leaderElected log entry appended only after the replicators are started,
        //because when there is only one node into the group, the replicators are used to directly update commit index.
        appendLeaderElected();
        scheduleStepDownTimeoutChecker();
    }

    @Override
    public void stop() {
        if (replicators != null) {
            replicators.stop();
            replicators = null;
        }
        pendingEntries.forEach((index, completableFuture) -> completableFuture
                .completeExceptionally(new IllegalStateException("Leader stepped down during processing of transaction")));
        pendingEntries.clear();
        logger.info("{} in term {}: {} steps down from Leader role.", groupId(), currentTerm(), me());
        if (scheduler.get() != null) {
            scheduler.getAndSet(null).shutdownNow();
        }
    }

    @Override
    public AppendEntriesResponse appendEntries(AppendEntriesRequest request) {
        if (request.getTerm() > currentTerm()) {
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
        return appendEntriesFailure(request.getRequestId(), "Request rejected because I'm a leader");
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
        return createEntry(currentTerm(), entryType, entryData);
    }

    private void scheduleStepDownTimeoutChecker() {
        scheduler.get().schedule(this::checkStepdown, maxElectionTimeout(), MILLISECONDS);
    }

    @Override
    public void forceStepDown() {
        String message = format("%s in term %s: Forced Step Down of %s.", groupId(), currentTerm(), me());
        logger.info(message);
        stepDown(message);
    }

    private void stepDown(String cause) {
        changeStateTo(stateFactory().followerState(), cause);
    }

    private void checkStepdown() {
        long otherNodesCount = otherNodesCount();
        if (otherNodesCount == 0) {
            scheduler.get().schedule(this::checkStepdown, maxElectionTimeout(), MILLISECONDS);
            return;
        }
        List<Long> lastTimeAgo = replicators.lastMessages();
        long lastTimeAgoFromMajority = lastTimeAgo.stream().skip((int)Math.floor((otherNodesCount-1d)/2)).findFirst().orElse(0L);
        if (lastTimeAgoFromMajority > maxElectionTimeout()) {
            String message = format("%s in term %s: StepDown as no messages received for %s ms (%s) other nodes(%d).",
                                    groupId(),
                                    currentTerm(),
                                    lastTimeAgoFromMajority,
                                    lastTimeAgo,
                                    otherNodesCount);
            logger.info(message);
            changeStateTo(stateFactory().followerState(), message);
        } else {
            logger.trace("{} in term {}: Reschedule checkStepdown after {}ms",
                         groupId(),
                         currentTerm(),
                         maxElectionTimeout() - lastTimeAgoFromMajority);
            scheduler.get().schedule(this::checkStepdown, maxElectionTimeout() - lastTimeAgoFromMajority, MILLISECONDS);
        }
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
                pendingEntries.put(e.getIndex(), appendEntryDone);
                try {
                    replicators.updateCommitIndex(e.getIndex());
                    replicators.notifySenders();
                } catch (Exception ex) {
                    logger.error("{} in term {}: problem happened during replicators update.", groupId(), currentTerm());
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
                logger.warn(
                        "Failed to retrieve waiting call for log entry index {} - entry = {}, could not complete the request.",
                        e.getIndex(),
                        e);
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
    public Iterator<ReplicatorPeer> replicatorPeers() {
        return replicators.replicatorPeerMap.values().iterator();
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

    private class Replicators {

        private final Set<String> nonVotingReplica = new CopyOnWriteArraySet<>();
        private final List<Registration> registrations = new ArrayList<>();
        private final Map<String, ReplicatorPeer> replicatorPeerMap = new ConcurrentHashMap<>();
        private final AtomicBoolean replicationRunning = new AtomicBoolean(false);

        void stop() {
            logger.info("{} in term {}: Stop replication thread", groupId(), currentTerm());
            long now = scheduler.get().clock().millis();
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

            notifySenders();
            registrations.forEach(Registration::cancel);
        }

        private void start() {
            registrations.add(registerConfigurationListener(this::updateNodes));
            try {
                otherPeersStream().forEach(peer -> registrations.add(registerPeer(peer, this::updateCommitIndex)));
                scheduler.get().execute(() -> replicate(false));
                logger.info("{} in term {}: Start replication thread for {} peers.",
                            groupId(),
                            currentTerm(),
                            replicatorPeerMap.size());
            } catch (RuntimeException re) {
                logger.warn("{} in term {}: Replication thread completed exceptionally.", groupId(), currentTerm(), re);
            }
        }

        private void replicate(boolean fromNotify) {
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
                                    sent += time(() -> raftPeer.sendNextMessage(fromNotify), raftPeer.nodeId());
                                }
                                if (sent == 0) {
                                    runsWithoutChanges++;
                                } else {
                                    runsWithoutChanges = 0;
                                }
                            }
                        } finally {
                            schedulerInstance.schedule(() -> replicate(false),
                                                       raftGroup().raftConfiguration().heartbeatTimeout()/2,
                                                       MILLISECONDS);
                            replicationRunning.set(false);
                        }
                    });
        }

        private int time(Supplier<Integer> operation, String target) {
            long before = System.currentTimeMillis();
            try {
                return operation.get();
            } finally {
                if( logger.isTraceEnabled()) {
                    long after = System.currentTimeMillis();
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
            if( ! replicationRunning.get()) {
                scheduler.get().execute(() -> replicate(true));
            }
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
                registrations.add(registerPeer(raftPeer, this::updateCommitIndex));
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
                                                               snapshotManager(),
                                                               LeaderState.this::updateCurrentTerm,
                                                               LeaderState.this::lastLogIndex);
            replicatorPeer.start();
            replicatorPeerMap.put(raftPeer.nodeId(), replicatorPeer);
            return () -> {
                ReplicatorPeer removed = replicatorPeerMap.remove(raftPeer.nodeId());
                if (removed != null) {
                    removed.stop();
                }
            };
        }

        public List<Long> lastMessages() {
            long now = System.currentTimeMillis();
            return replicatorPeerMap.values().stream().map(peer -> now - peer.lastMessageReceived()).sorted().collect(Collectors.toList());
        }
    }
}
