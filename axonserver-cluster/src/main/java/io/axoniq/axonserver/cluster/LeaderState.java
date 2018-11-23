package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.Scheduler.ScheduledRegistration;
import io.axoniq.axonserver.cluster.replication.EntryIterator;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesRequest;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesResponse;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotRequest;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotResponse;
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
    private static final long MAX_ENTRIES_PER_BATCH = 100;

    private static final Logger logger = LoggerFactory.getLogger(LeaderState.class);
    private final AtomicReference<ScheduledRegistration> stepDown = new AtomicReference<>();

    private final Map<Long, CompletableFuture<Void>> pendingEntries = new ConcurrentHashMap<>();
    private final ExecutorService executor = Executors.newCachedThreadPool(r -> {
        Thread t= new Thread(r);
        t.setName("Replication-" + LeaderState.this.raftGroup().raftConfiguration().groupId());
        return t;
    });
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
        long now = clock.millis();
        long lastReceived = replicators.lastMessageTimeFromMajority();
        if( now - lastReceived > 10*maxElectionTimeout()) {
            logger.info("StepDown as no messages received for {}ms", (now-lastReceived));
            changeStateTo(stateFactory().followerState());
        } else {
            logger.trace("Reschedule stepDown after {}ms", maxElectionTimeout() - (now-lastReceived));
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

    private class Replicators {
        private volatile boolean running = true;
        private volatile Thread workingThread;
        private final List<Registration> registrations = new ArrayList<>();
        private final Map<String, ReplicatorPeer> replicatorPeerMap = new ConcurrentHashMap<>();

        void stop() {
            logger.info("{}: Stop replication thread", me());
            replicatorPeerMap.forEach((peer,replicator) -> {
                logger.info("{}: MatchIndex = {}, NextIndex = {}", peer, replicator.matchIndex, replicator.nextIndex);
            });

            running = false;
            notifySenders(null);
            registrations.forEach(Registration::cancel);
            workingThread = null;
        }

        void start() {
            workingThread = Thread.currentThread();
            try {
                TermIndex lastTermIndex = raftGroup().localLogEntryStore().lastLog();
                otherNodesStream().forEach(raftPeer -> {

                    ReplicatorPeer replicatorPeer = new ReplicatorPeer(raftPeer,
                                                                       this::updateMatchIndex);
                    replicatorPeerMap.put(raftPeer.nodeId(), replicatorPeer);
                    registrations.add(raftPeer.registerAppendEntriesResponseListener(replicatorPeer::handleResponse));
                    registrations.add(raftPeer.registerInstallSnapshotResponseListener(replicatorPeer::handleResponse));
                });

                long commitIndex = raftGroup().localLogEntryStore().commitIndex();
                replicatorPeerMap.forEach((nodeId, peer) -> peer.sendHeartbeat(lastTermIndex, commitIndex));
                logger.info("{}: Start replication thread for {} peers", me(), replicatorPeerMap.size());

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
            long nextCommitCandidate = raftGroup().localLogEntryStore().commitIndex() + 1;
            if( matchIndex < nextCommitCandidate) return;
            while( matchedByMajority( nextCommitCandidate)) {
                raftGroup().localLogEntryStore().markCommitted(nextCommitCandidate);
                nextCommitCandidate++;
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
                raftGroup().localLogEntryStore().markCommitted(entry.getIndex());
            }
        }

        private long lastMessageTimeFromMajority() {
            logger.trace("Last messages received: {}", replicatorPeerMap.values().stream().map(peer -> peer.lastMessageSent).collect(
                    Collectors.toList()));
            return replicatorPeerMap.values().stream().map(r -> r.lastMessageReceived)
                                    .sorted()
                                    .skip((int)Math.floor(otherNodesCount() / 2f)).findFirst().orElse(0L);
        }

    }

    private class ReplicatorPeer {
        private final RaftPeer raftPeer;
        private final Consumer<Long> matchIndexCallback;
        private volatile EntryIterator entryIterator;
        private final AtomicLong nextIndex = new AtomicLong(0);
        private final AtomicLong matchIndex = new AtomicLong(0);
        private volatile long lastMessageSent = 0L;
        private volatile long lastMessageReceived = 0L;
        private boolean replyReceived = false;

        public ReplicatorPeer(RaftPeer raftPeer, Consumer<Long> matchIndexCallback) {
            this.raftPeer = raftPeer;
            this.matchIndexCallback = matchIndexCallback;
        }

        private boolean canSend() {
            return nextIndex.get() - matchIndex.get() < FLOW_BUFFER;
        }

        public int sendNextEntries() {
            int sent = 0;
            try {
                if (entryIterator == null) {
                    nextIndex.compareAndSet(0, raftGroup().localLogEntryStore().lastLogIndex()+1);
                    logger.info("{}: create entry iterator for {} at {}", me(), raftPeer.nodeId(), nextIndex);
                    entryIterator = raftGroup().localLogEntryStore().createIterator(nextIndex.get());
                }

                logger.trace("{} Has entries for {}: {}", me(), raftPeer.nodeId(), entryIterator.hasNext());
                if( !canSend()) {
                    logger.trace("{}: nextIndex: {}, matchIndex: {}", raftPeer.nodeId(), nextIndex, matchIndex);
                }
                while (canSend()
                        &&  sent < MAX_ENTRIES_PER_BATCH && entryIterator.hasNext()) {
                    Entry entry = entryIterator.next();
                    //
                    TermIndex previous = entryIterator.previous();
                    send(AppendEntriesRequest.newBuilder()
                                             .setGroupId(groupId())
                                             .setPrevLogIndex(previous == null ? 0 : previous.getIndex())
                                             .setPrevLogTerm(previous == null ? 0 : previous.getTerm())
                                             .setTerm(currentTerm())
                                             .setLeaderId(me())
                            .setCommitIndex(raftGroup().localLogEntryStore().commitIndex())
                            .addEntries(entry)
                            .build());
                    nextIndex.incrementAndGet();
                    sent++;
                }

                long now = clock.millis();
                if( sent == 0 && now - lastMessageSent > raftGroup().raftConfiguration().heartbeatTimeout()) {
                    TermIndex termIndex = raftGroup().localLogEntryStore().lastLog();
                    sendHeartbeat(termIndex, raftGroup().localLogEntryStore().commitIndex());
                } else {
                    if( sent > 0) {
                        lastMessageSent = clock.millis();
                    }
                }
            } catch( Throwable ex) {
                logger.warn("{}: Sending nextEntries to {} failed", me(), raftPeer.nodeId(), ex);
            }
            return sent;
        }
        private void sendHeartbeat(TermIndex lastTermIndex, long commitIndex) {
            AppendEntriesRequest heartbeat = AppendEntriesRequest.newBuilder()
                    .setCommitIndex(commitIndex)
                    .setLeaderId(me())
                    .setGroupId(raftGroup().raftConfiguration().groupId())
                    .setTerm(raftGroup().localElectionStore().currentTerm())
//                    .setPrevLogIndex(lastTermIndex.getIndex())
//                    .setPrevLogTerm(lastTermIndex.getTerm())
                    .build();
            send(heartbeat);
            lastMessageSent = clock.millis();
        }

        private void send(AppendEntriesRequest request) {
            logger.trace("{}: Send request to {}: {}", me(), raftPeer.nodeId(), request);
            raftPeer.appendEntries(request);
        }

        public long getMatchIndex() {
            return matchIndex.get();
        }

        public void handleResponse(AppendEntriesResponse appendEntriesResponse) {
            lastMessageReceived = clock.millis();
            replyReceived = true;
            logger.trace("{}: Received response from {}: {}", me(), raftPeer.nodeId(), appendEntriesResponse);
            if (appendEntriesResponse.hasFailure()) {
                logger.warn("{}: Received failed response from {}: {}", me(), raftPeer.nodeId(), appendEntriesResponse);
                if( currentTerm()  < appendEntriesResponse.getTerm()) {
                    logger.warn("Replica has higher term: {}", appendEntriesResponse.getTerm());
                    //updateCurrentTerm(appendEntriesResponse.getTerm());
                    changeStateTo(stateFactory().followerState());
                    return;
                }
                logger.warn("{}: create entry iterator as replica does not have current for {} at {}, lastSaved = {}", me(), raftPeer.nodeId(), nextIndex,
                            appendEntriesResponse.getFailure().getLastAppliedIndex());
                matchIndex.set(appendEntriesResponse.getFailure().getLastAppliedIndex());
                nextIndex.set(appendEntriesResponse.getFailure().getLastAppliedIndex() + 1);
                entryIterator =  raftGroup().localLogEntryStore().createIterator(nextIndex.get());;
            } else {
                if (appendEntriesResponse.getSuccess().getLastLogIndex() > matchIndex.get()) {
                    matchIndex.set(appendEntriesResponse.getSuccess().getLastLogIndex());
                    matchIndexCallback.accept(matchIndex.get());
                } else {
                    if( appendEntriesResponse.getSuccess().getLastLogIndex() == 0 ) {
                        nextIndex.set(appendEntriesResponse.getFailure().getLastAppliedIndex() + 1);
                        logger.warn("{}: create entry iterator as empty replica for {} at {}", me(), raftPeer.nodeId(), nextIndex);
                        entryIterator =  raftGroup().localLogEntryStore().createIterator(nextIndex.get());;
                    }
                }
            }
        }

        public void handleResponse(InstallSnapshotResponse installSnapshotResponse) {

        }
    }
}
