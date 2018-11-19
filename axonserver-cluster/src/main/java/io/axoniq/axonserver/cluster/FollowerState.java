package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.Scheduler.ScheduledRegistration;
import io.axoniq.axonserver.cluster.replication.LogEntryStore;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesRequest;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesResponse;
import io.axoniq.axonserver.grpc.cluster.AppendEntrySuccess;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotRequest;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotResponse;
import io.axoniq.axonserver.grpc.cluster.RequestVoteRequest;
import io.axoniq.axonserver.grpc.cluster.RequestVoteResponse;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static java.lang.Math.min;

public class FollowerState extends AbstractMembershipState {

    private final Scheduler scheduler;

    private AtomicReference<ScheduledRegistration> scheduledElection = new AtomicReference<>();
    private volatile boolean heardFromLeader;

    protected FollowerState(Builder builder) {
        super(builder);
        this.scheduler = builder.scheduler;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void start() {
        cancelCurrentElectionTimeout();
        scheduleNewElection();
        heardFromLeader = false;
    }

    @Override
    public void stop() {
        cancelCurrentElectionTimeout();
    }

    @Override
    public AppendEntriesResponse appendEntries(AppendEntriesRequest request) {
        updateCurrentTerm(request.getTerm());
        rescheduleElection(request.getTerm());
        LogEntryStore logEntryStore = raftGroup().localLogEntryStore();

        //1. Reply false if term < currentTerm
        if (request.getTerm() < currentTerm()) {
            return appendEntriesFailure();
        }

        //2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
        if (!logEntryStore.contains(request.getPrevLogIndex(), request.getPrevLogTerm())) {
            return appendEntriesFailure();
        }

        //3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry
        // and all that follow it
        //4. Append any new entries not already in the log
        try {
            raftGroup().localLogEntryStore().appendEntry(request.getEntriesList());
        } catch (IOException e) {
            stop();
            return appendEntriesFailure();
        }

        //5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
        if (request.getCommitIndex() > logEntryStore.commitIndex()) {
            logEntryStore.markCommitted(min(request.getCommitIndex(),
                                            request.getEntries(request.getEntriesCount() - 1).getIndex()));
        }

        return AppendEntriesResponse.newBuilder()
                                    .setGroupId(groupId())
                                    .setTerm(currentTerm())
                                    .setSuccess(buildAppendEntrySuccess(lastLogIndex()))
                                    .setTerm(currentTerm())
                                    .build();
    }

    @Override
    public RequestVoteResponse requestVote(RequestVoteRequest request) {
        long elapsedFromLastMessage = scheduledElection.get().getElapsed(TimeUnit.MILLISECONDS);

        // If a server receives a RequestVote within the minimum election timeout of hearing from a current leader, it
        // does not update its term or grant its vote
        if (heardFromLeader && elapsedFromLastMessage < minElectionTimeout()) {
            return requestVoteResponse(false);
        }

        updateCurrentTerm(request.getTerm());
        return requestVoteResponse(voteGrantedFor(request));
    }

    @Override
    public InstallSnapshotResponse installSnapshot(InstallSnapshotRequest request) {
        throw new NotImplementedException();
    }

    private void cancelCurrentElectionTimeout() {
        Optional.ofNullable(scheduledElection.get())
                .ifPresent(Registration::cancel);
    }

    private void scheduleNewElection() {
        scheduledElection.set(scheduler.schedule(
                () -> changeStateTo(stateFactory().candidateState()),
                ThreadLocalRandom.current().nextLong(minElectionTimeout(), maxElectionTimeout() + 1),
                TimeUnit.MILLISECONDS));
    }

    private void rescheduleElection(long term) {
        if (term >= currentTerm()) {
            heardFromLeader = true;
            cancelCurrentElectionTimeout();
            scheduleNewElection();
        }
    }

    private AppendEntrySuccess buildAppendEntrySuccess(long lastLogIndex) {
        return AppendEntrySuccess.newBuilder()
                                 .setLastLogIndex(lastLogIndex)
                                 .build();
    }

    private boolean voteGrantedFor(RequestVoteRequest request) {
        //1. Reply false if term < currentTerm
        if (request.getTerm() < currentTerm()) {
            return false;
        }

        //2. If votedFor is null or candidateId, and candidate's log is at least as up-to-date as receiver's log, grant
        // vote
        String votedFor = votedFor();
        if (votedFor != null && !votedFor.equals(request.getCandidateId())) {
            return false;
        }

        if (request.getLastLogTerm() < lastLogTerm()) {
            return false;
        }

        if (request.getLastLogIndex() < lastLogIndex()) {
            return false;
        }

        markVotedFor(request.getCandidateId());
        return true;
    }

    public static class Builder extends AbstractMembershipState.Builder<Builder> {

        private Scheduler scheduler = new DefaultScheduler();

        public Builder scheduler(Scheduler scheduler) {
            this.scheduler = scheduler;
            return this;
        }

        public FollowerState build() {
            return new FollowerState(this);
        }
    }
}
