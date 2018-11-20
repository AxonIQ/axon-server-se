package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.Scheduler.ScheduledRegistration;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesRequest;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesResponse;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotRequest;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotResponse;
import io.axoniq.axonserver.grpc.cluster.RequestVoteRequest;
import io.axoniq.axonserver.grpc.cluster.RequestVoteResponse;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Author: marc
 */
public class LeaderState extends AbstractMembershipState {

    private final AtomicReference<ScheduledRegistration> stepDown = new AtomicReference<>();
    private final AtomicReference currentHeartbeatRound = new AtomicReference();

    private final Map<Long, CompletableFuture<Void>> pendingEntries = new ConcurrentHashMap<>();
    private final ExecutorService executor = Executors.newCachedThreadPool(r -> {
        Thread t= new Thread(r);
        t.setName("Replication-" + LeaderState.this.raftGroup().raftConfiguration().groupId());
        return t;
    });
    private volatile Replicators replicators;
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
    }

    @Override
    public void start() {
        scheduleStepDown();
        replicators = new Replicators();
        executor.submit(() -> replicators.start());
    }

    @Override
    public void stop() {
        cancelStepDown();
        pendingEntries.forEach((idx, future) -> future.completeExceptionally(new IllegalStateException()));
        pendingEntries.clear();
        replicators.stop();
        replicators = null;
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
        long elapsedFromLastConfirmedHeartbeat = stepDown.get().getElapsed(MILLISECONDS);
        if (elapsedFromLastConfirmedHeartbeat > minElectionTimeout()){
            handleAsFollower(follower -> follower.requestVote(request));
        }
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
        changeStateTo(stateFactory().followerState());
    }

    private void resetStepDown(){
        cancelStepDown();
        scheduleStepDown();
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

        void stop() {
            running = false;
            notifySenders(null);
            registrations.forEach(Registration::cancel);
            workingThread = null;
        }

        void start() {
            workingThread = Thread.currentThread();
            TermIndex lastTermIndex = raftGroup().localLogEntryStore().lastLog();
            registrations.addAll(otherNodesStream().map(raftPeer ->
                raftPeer.registerMatchIndexListener(this::updateMatchIndex)
            ).collect(Collectors.toList()));

            long commitIndex = raftGroup().localLogEntryStore().commitIndex();
            otherNodes().forEach(peer-> sendHeartbeat(peer, lastTermIndex, commitIndex));

            while( running) {
                int runsWithoutChanges = 0;
                    while( runsWithoutChanges < 10) {
                        int sent = 0;
                        for (RaftPeer raftPeer : otherNodes()) {
                            sent += raftPeer.sendNextEntries();
                        }

                        if( sent == 0) {
                            runsWithoutChanges++ ;
                        } else {
                            runsWithoutChanges = 0;
                        }
                    }
                    LockSupport.parkNanos(1000);
            }
        }

        private void sendHeartbeat(RaftPeer peer, TermIndex lastTermIndex, long commitIndex) {
            AppendEntriesRequest heartbeat = AppendEntriesRequest.newBuilder()
                                                                 .setCommitIndex(commitIndex)
                                                                 .setTerm(raftGroup().localElectionStore().currentTerm())
                                                                 .setPrevLogIndex(lastTermIndex.getIndex())
                                                                 .setPrevLogTerm(lastTermIndex.getTerm())
                                                                 .build();
//                                                                 .setGroupId()
//                    .setLeaderId(raftGroup().localNode().)

            peer.send(heartbeat);
        }

        private void updateMatchIndex(long matchIndex) {
            System.out.println("Updated matchIndex: " + matchIndex);
            long nextCommitCandidate = raftGroup().localLogEntryStore().commitIndex() + 1;
            if( matchIndex < nextCommitCandidate) return;
            while( matchedByMajority( nextCommitCandidate)) {
                raftGroup().localLogEntryStore().markCommitted(nextCommitCandidate);
                nextCommitCandidate++;
            }

        }

        private boolean matchedByMajority(long nextCommitCandidate) {
            int majority = (int) Math.ceil(otherNodesCount() / 2f);
            return otherNodesStream().filter(p -> p.getMatchIndex() >= nextCommitCandidate).count() >= majority;
        }

        void notifySenders(Entry entry) {
            if( workingThread != null)
                LockSupport.unpark(workingThread);

            if( entry != null && otherNodesCount() == 0) {
                raftGroup().localLogEntryStore().markCommitted(entry.getIndex());
            }
        }


    }
}
