package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.replication.LogEntryStore;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesRequest;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesResponse;
import io.axoniq.axonserver.grpc.cluster.AppendEntryFailure;
import io.axoniq.axonserver.grpc.cluster.AppendEntrySuccess;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotRequest;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotResponse;
import io.axoniq.axonserver.grpc.cluster.RequestVoteRequest;
import io.axoniq.axonserver.grpc.cluster.RequestVoteResponse;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static java.lang.Math.min;

public class FollowerState extends AbstractMembershipState {

    private final long electionTimeoutMin;
    private final long electionTimeoutMax;

    private final ScheduledExecutorService scheduledExecutorService;

    private ScheduledFuture<?> scheduledElection;
    private Long lastMessageReceivedAt;

    protected FollowerState(Builder builder) {
        super(builder);
        this.scheduledExecutorService = builder.scheduledExecutorService;
        this.electionTimeoutMin = builder.electionTimeoutMin;
        this.electionTimeoutMax = builder.electionTimeoutMax;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void start() {
        scheduleNewElection();
    }

    @Override
    public synchronized void stop() {
        cancelCurrentElectionTimeout();
        scheduledExecutorService.shutdown();
    }

    @Override
    public AppendEntriesResponse appendEntries(AppendEntriesRequest request) {
        newMessageReceived(request.getTerm());
        LogEntryStore logEntryStore = raftGroup().localLogEntryStore();
        long lastAppliedIndex = logEntryStore.lastAppliedIndex();
        AppendEntriesResponse.Builder responseBuilder = AppendEntriesResponse.newBuilder()
                                                                             .setGroupId(request.getGroupId())
                                                                             .setTerm(currentTerm());

        //1. Reply false if term < currentTerm
        if (request.getTerm() < currentTerm()) {
            return responseBuilder.setFailure(buildAppendEntryFailure(lastAppliedIndex))
                                  .build();
        }

        //2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
        if (!logEntryStore.contains(request.getPrevLogIndex(), request.getPrevLogTerm())) {
            return responseBuilder.setFailure(buildAppendEntryFailure(lastAppliedIndex))
                                  .build();
        }

        //3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry
        // and all that follow it
        //4. Append any new entries not already in the log
        try {
            raftGroup().localLogEntryStore().appendEntry(request.getEntriesList());
            lastAppliedIndex = logEntryStore.lastAppliedIndex();
        } catch (IOException e) {
            stop();
            return responseBuilder.setFailure(buildAppendEntryFailure(lastAppliedIndex))
                                  .build();
        }

        //5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
        if (request.getCommitIndex() > logEntryStore.commitIndex()) {
            logEntryStore.markCommitted(min(request.getCommitIndex(),
                                            request.getEntries(request.getEntriesCount() - 1).getIndex()));
        }

        return responseBuilder.setSuccess(buildAppendEntrySuccess(lastAppliedIndex))
                              .setTerm(currentTerm())
                              .build();
    }

    @Override
    public RequestVoteResponse requestVote(RequestVoteRequest request) {
        long elapsedFromLastMessage = System.currentTimeMillis() - lastMessageReceivedAt;
        newMessageReceived(request.getTerm());

        return RequestVoteResponse.newBuilder()
                                  .setGroupId(request.getGroupId())
                                  .setVoteGranted(voteGrantedFor(request, elapsedFromLastMessage))
                                  .setTerm(currentTerm())
                                  .build();
    }

    @Override
    public InstallSnapshotResponse installSnapshot(InstallSnapshotRequest request) {
        newMessageReceived(request.getTerm());
        throw new NotImplementedException();
    }

    private void cancelCurrentElectionTimeout() {
        if (scheduledElection != null) {
            scheduledElection.cancel(true);
        }
    }

    private void scheduleNewElection() {
        scheduledElection = scheduledExecutorService.schedule(() -> {
            stop();
//            transition(new CandidateState());
            // TODO: make ThreadLocalRandom injectable
        }, ThreadLocalRandom.current().nextLong(electionTimeoutMin, electionTimeoutMax + 1), TimeUnit.MILLISECONDS);
    }

    private void rescheduleElection() {
        cancelCurrentElectionTimeout();
        scheduleNewElection();
    }

    private void newMessageReceived(long term) {
        lastMessageReceivedAt = System.currentTimeMillis();
        updateCurrentTerm(term);
        rescheduleElection();
    }

    private AppendEntryFailure buildAppendEntryFailure(long lastAppliedIndex) {
        return AppendEntryFailure.newBuilder()
                                 .setLastAppliedIndex(lastAppliedIndex)
                                 .setLastAppliedEventSequence(lastAppliedEventSequenceSupplier().get())
                                 .build();
    }

    private AppendEntrySuccess buildAppendEntrySuccess(long lastAppliedIndex) {
        return AppendEntrySuccess.newBuilder()
                                 .setLastLogIndex(lastAppliedIndex)
                                 .build();
    }

    private boolean voteGrantedFor(RequestVoteRequest request, long elapsedFromLastMessage) {
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

        // If a server receives a RequestVote within the minimum election timeout of hearing from a current leader, it
        // does not update its term or grant its vote
        if (elapsedFromLastMessage < electionTimeoutMin) {
            return false;
        }

        markVotedFor(request.getCandidateId());
        return true;
    }

    public static class Builder extends AbstractMembershipState.Builder {

        private long electionTimeoutMin = 150;
        private long electionTimeoutMax = 300;

        private ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

        @Override
        public Builder raftGroup(RaftGroup raftGroup) {
            super.raftGroup(raftGroup);
            return this;
        }

        @Override
        public Builder transitionHandler(Consumer<MembershipState> transitionHandler) {
            super.transitionHandler(transitionHandler);
            return this;
        }

        @Override
        public Builder lastAppliedEventSequenceSupplier(
                Supplier<Long> lastAppliedEventSequenceSupplier) {
            super.lastAppliedEventSequenceSupplier(lastAppliedEventSequenceSupplier);
            return this;
        }

        public Builder electionTimeoutMin(long electionTimeoutMin) {
            this.electionTimeoutMin = electionTimeoutMin;
            return this;
        }

        public Builder electionTimeoutMax(long electionTimeoutMax) {
            this.electionTimeoutMax = electionTimeoutMax;
            return this;
        }

        public Builder scheduledExecutorService(ScheduledExecutorService scheduledExecutorService) {
            this.scheduledExecutorService = scheduledExecutorService;
            return this;
        }

        public FollowerState build() {
            return new FollowerState(this);
        }
    }
}
