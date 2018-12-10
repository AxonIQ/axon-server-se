package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.Scheduler.ScheduledRegistration;
import io.axoniq.axonserver.cluster.replication.EntryIterator;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Author: marc
 */
public class LeaderState extends AbstractMembershipState {
    private static final long FLOW_BUFFER = 100;
    private static final long MAX_ENTRIES_PER_BATCH = 10;

    private static final Logger logger = LoggerFactory.getLogger(LeaderState.class);
    private final AtomicReference<ScheduledRegistration> stepDown = new AtomicReference<>();

    private final Map<Long, CompletableFuture<Void>> pendingEntries = new ConcurrentHashMap<>();
    private final ExecutorService executor = Executors.newCachedThreadPool(new AxonThreadFactory("Replicator-"));
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
        pendingEntries.forEach((idx, future) -> future.completeExceptionally(new IllegalStateException()));
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
        ScheduledRegistration newTask = scheduler().schedule(this::stepDown, maxElectionTimeout(), MILLISECONDS);
        stepDown.set(newTask);
    }

    private void cancelStepDown(){
        stepDown.get().cancel();
    }

    private void stepDown() {
        if( otherNodes().isEmpty()) return;
        long now = clock.millis();
        long lastReceived = replicators.lastMessageTimeFromMajority();
        if( now - lastReceived > maxElectionTimeout()) {
            logger.info("{}: StepDown as no messages received for {}ms", groupId(), (now-lastReceived));
            changeStateTo(stateFactory().followerState());
        } else {
            logger.trace("{}: Reschedule stepDown after {}ms", groupId(), maxElectionTimeout() - (now-lastReceived));
            ScheduledRegistration newTask = scheduler().schedule(this::stepDown, maxElectionTimeout() - (now-lastReceived), MILLISECONDS);
            stepDown.set(newTask);
        }

    }

    private CompletableFuture<Void> createEntry(long currentTerm, String entryType, byte[] entryData) {
        CompletableFuture<Void> appendEntryDone = new CompletableFuture<>();
        CompletableFuture<Entry> entryFuture = raftGroup().localLogEntryStore().createEntry(currentTerm, entryType, entryData);
        entryFuture.whenComplete((e, failure) -> {
            if( failure != null) {
                appendEntryDone.completeExceptionally(failure);
            } else {
                if( replicators != null) {
                    replicators.notifySenders(e);
                }
                pendingEntries.put(e.getIndex(), appendEntryDone);
            }
        });
        return appendEntryDone;
    }

    @Override
    public void applied(Entry e) {
        CompletableFuture<Void> pendingEntry = pendingEntries.remove(e.getIndex());
        if( pendingEntry != null) {
            pendingEntry.complete(null);
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
        private final List<Registration> registrations = new ArrayList<>();
        private final Map<String, ReplicatorPeer> replicatorPeerMap = new ConcurrentHashMap<>();

        void stop() {
            logger.info("{}: Stop replication thread", groupId());
            replicatorPeerMap.forEach((peer,replicator) -> logger.info("{}: MatchIndex = {}, NextIndex = {}", peer, replicator.matchIndex, replicator.nextIndex));
            logger.info("{}: last applied: {}", groupId(), raftGroup().logEntryProcessor().lastAppliedIndex());

            running = false;
            notifySenders(null);
            registrations.forEach(Registration::cancel);
            workingThread = null;
        }

        

        void start() {
            workingThread = Thread.currentThread();
            try {

                otherNodesStream().forEach(raftPeer -> {

                    ReplicatorPeer replicatorPeer = new ReplicatorPeer(raftPeer,
                                                                       this::updateMatchIndex);
                    replicatorPeerMap.put(raftPeer.nodeId(), replicatorPeer);
                    registrations.add(raftPeer.registerAppendEntriesResponseListener(replicatorPeer::handleResponse));
                    registrations.add(raftPeer.registerInstallSnapshotResponseListener(replicatorPeer::handleResponse));
                });

                long commitIndex = raftGroup().logEntryProcessor().commitIndex();
                TermIndex lastTermIndex = raftGroup().localLogEntryStore().lastLog();
                replicatorPeerMap.forEach((nodeId, peer) -> peer.sendHeartbeat( lastTermIndex, commitIndex));
                logger.info("{}: Start replication thread for {} peers", groupId(), replicatorPeerMap.size());

                while (running) {
                    int runsWithoutChanges = 0;
                    while (running && runsWithoutChanges < 3) {
                        int sent = 0;
                        for (ReplicatorPeer raftPeer : replicatorPeerMap.values()) {
                            sent += raftPeer.sendNextEntries();
                        }

                        if (sent == 0) {
                            runsWithoutChanges++;
                        } else {
                            runsWithoutChanges = 0;
                        }
                    }
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(5));
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
            for( long index = nextCommitCandidate ; index  <= matchIndex; index++) {
                if (matchedByMajority(index)) {
                    nextCommitCandidate=index;
                    updateCommit=true;
                }
            }

            if( updateCommit &&
                    raftGroup().localLogEntryStore().getEntry(nextCommitCandidate).getTerm() == raftGroup().localElectionStore().currentTerm()) {
                raftGroup().logEntryProcessor().markCommitted(nextCommitCandidate);
            }

        }

        private boolean matchedByMajority(long nextCommitCandidate) {
            int majority = (int) Math.ceil(otherNodesCount() / 2f);
            return replicatorPeerMap.values().stream().filter(p -> p.getMatchIndex() >= nextCommitCandidate).count() >= majority;
        }

        void notifySenders(Entry entry) {
            if( workingThread != null)
                LockSupport.unpark(workingThread);

            if( entry != null && otherNodesCount() == 0) {
                raftGroup().logEntryProcessor().markCommitted(entry.getIndex());
            }
        }

        private long lastMessageTimeFromMajority() {
            logger.trace("Last messages received: {}", replicatorPeerMap.values().stream().map(peer -> peer.lastMessageReceived).collect(
                    Collectors.toList()));
            return replicatorPeerMap.values().stream().map(r -> r.lastMessageReceived)
                                    .sorted()
                                    .skip((int)Math.floor(otherNodesCount() / 2f)).findFirst().orElse(0L);
        }

        public void addNode(Node node) {
            if( node.getNodeId() != me()) {
                RaftPeer raftPeer = raftGroup().peer(node.getNodeId());
                ReplicatorPeer replicatorPeer = new ReplicatorPeer(raftPeer, this::updateMatchIndex);
                replicatorPeerMap.put(raftPeer.nodeId(), replicatorPeer);
                registrations.add(raftPeer.registerAppendEntriesResponseListener(replicatorPeer::handleResponse));
                registrations.add(raftPeer.registerInstallSnapshotResponseListener(replicatorPeer::handleResponse));
            }
            appendConfiguration();
        }

        public void removeNode(String nodeId) {
            replicatorPeerMap.remove(nodeId);
            appendConfiguration();
        }
    }


    private class ReplicatorPeer {
        private final RaftPeer raftPeer;
        private final Consumer<Long> matchIndexCallback;
        private volatile EntryIterator entryIterator;
        private final AtomicLong nextIndex = new AtomicLong(0);
        private final AtomicLong matchIndex = new AtomicLong(0);
        private volatile long lastMessageSent = 0L;
        private volatile long lastMessageReceived;

        public ReplicatorPeer(RaftPeer raftPeer, Consumer<Long> matchIndexCallback) {
            this.raftPeer = raftPeer;
            this.matchIndexCallback = matchIndexCallback;
            lastMessageReceived = clock.millis();
        }

        private boolean canSend() {
            return matchIndex.get() == 0 || nextIndex.get() - matchIndex.get() < FLOW_BUFFER;
        }

        public int sendNextEntries() {
            int sent = 0;
            try {
                if (entryIterator == null) {
                    nextIndex.compareAndSet(0, raftGroup().localLogEntryStore().lastLogIndex()+1);
                    logger.debug("{}: create entry iterator for {} at {}", groupId(), raftPeer.nodeId(), nextIndex);
                    entryIterator = raftGroup().localLogEntryStore().createIterator(nextIndex.get());
                }

                if( !canSend()) {
                    logger.trace("{}: Trying to send to {} (nextIndex = {}, matchIndex = {}, lastLog = {})",
                                groupId(),
                                raftPeer.nodeId(),
                                nextIndex,
                                matchIndex,
                                raftGroup().localLogEntryStore().lastLogIndex());
                }
                while (canSend()
                        &&  sent < MAX_ENTRIES_PER_BATCH && entryIterator.hasNext()) {
                    Entry entry = entryIterator.next();
                    //
                    TermIndex previous = entryIterator.previous();
                    logger.trace("{}: Send request {} to {}: {}", groupId(), sent, raftPeer.nodeId(), entry.getIndex());
                    send(AppendEntriesRequest.newBuilder()
                                             .setGroupId(groupId())
                                             .setPrevLogIndex(previous == null ? 0 : previous.getIndex())
                                             .setPrevLogTerm(previous == null ? 0 : previous.getTerm())
                                             .setTerm(currentTerm())
                                             .setLeaderId(me())
                            .setCommitIndex(raftGroup().logEntryProcessor().commitIndex())
                            .addEntries(entry)
                            .build());
                    nextIndex.set(entry.getIndex()+1);
                    sent++;
                }

                long now = clock.millis();
                if( sent == 0 && now - lastMessageSent > raftGroup().raftConfiguration().heartbeatTimeout()) {
                    sendHeartbeat( raftGroup().localLogEntryStore().lastLog(), raftGroup().logEntryProcessor().commitIndex());
                } else {
                    if( sent > 0) {
                        lastMessageSent = clock.millis();
                    }
                }
            } catch( RuntimeException ex) {
                logger.warn("{}: Sending nextEntries to {} failed", groupId(), raftPeer.nodeId(), ex);
            }
            return sent;
        }

        private void sendHeartbeat( TermIndex lastTermIndex, long commitIndex) {
            AppendEntriesRequest heartbeat = AppendEntriesRequest.newBuilder()
                    .setCommitIndex(commitIndex)
                    .setLeaderId(me())
                    .setGroupId(raftGroup().raftConfiguration().groupId())
                    .setTerm(raftGroup().localElectionStore().currentTerm())
                    .setPrevLogTerm(lastTermIndex.getTerm())

                                                                 .setPrevLogIndex(lastTermIndex.getIndex())
                    .build();
            send(heartbeat);
        }

        private void send(AppendEntriesRequest request) {
            logger.trace("{}: Send request to {}: {}", groupId(), raftPeer.nodeId(), request);
            raftPeer.appendEntries(request);
            lastMessageSent = clock.millis();
        }

        public long getMatchIndex() {
            return matchIndex.get();
        }

        public void handleResponse(AppendEntriesResponse appendEntriesResponse) {
            lastMessageReceived = clock.millis();
            logger.trace("{}: Received response from {}: {}", groupId(), raftPeer.nodeId(), appendEntriesResponse);
            if (appendEntriesResponse.hasFailure()) {
                if( currentTerm()  < appendEntriesResponse.getTerm()) {
                    logger.info("{}: Replica has higher term: {}", groupId(), appendEntriesResponse.getTerm());
                    updateCurrentTerm(appendEntriesResponse.getTerm());
                    changeStateTo(stateFactory().followerState());
                    return;
                }
                logger.debug("{}: create entry iterator as replica does not have current for {} at {}, lastSaved = {}", groupId(), raftPeer.nodeId(), nextIndex,
                            appendEntriesResponse.getFailure().getLastAppliedIndex());
                matchIndex.set(appendEntriesResponse.getFailure().getLastAppliedIndex());
                nextIndex.set(appendEntriesResponse.getFailure().getLastAppliedIndex() + 1);
                entryIterator =  raftGroup().localLogEntryStore().createIterator(nextIndex.get());
            } else {
                matchIndex.set(appendEntriesResponse.getSuccess().getLastLogIndex());
                matchIndexCallback.accept(matchIndex.get());

            }
        }

        public void handleResponse(InstallSnapshotResponse installSnapshotResponse) {

        }
    }
}
