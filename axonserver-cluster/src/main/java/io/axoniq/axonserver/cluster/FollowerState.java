package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.election.ElectionStore;
import io.axoniq.axonserver.cluster.replication.IncorrectTermException;
import io.axoniq.axonserver.cluster.replication.LogEntryStore;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesRequest;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesResponse;
import io.axoniq.axonserver.grpc.cluster.AppendEntryFailure;
import io.axoniq.axonserver.grpc.cluster.AppendEntrySuccess;
import io.axoniq.axonserver.grpc.cluster.Entry;
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

import static java.lang.Math.min;

public class FollowerState implements MembershipState {

    private final long electionTimeoutMin;
    private final long electionTimeoutMax;

    private final RaftGroup raftGroup;
    private final Consumer<MembershipState> transitionHandler;
    private final ScheduledExecutorService scheduledExecutorService;

    private ScheduledFuture<?> scheduledElection;

    protected FollowerState(Builder builder) {
        builder.validate();
        this.raftGroup = builder.raftGroup;
        this.transitionHandler = builder.transitionHandler;
        this.scheduledExecutorService = builder.scheduledExecutorService;
        this.electionTimeoutMin = builder.electionTimeoutMin;
        this.electionTimeoutMax = builder.electionTimeoutMax;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public synchronized void stop() {
        onExit();
        transitionHandler.accept(new IdleState(raftGroup, transitionHandler));
    }

    @Override
    public AppendEntriesResponse appendEntries(AppendEntriesRequest request) {
        AppendEntriesResponse.Builder responseBuilder = AppendEntriesResponse.newBuilder()
                                                                             .setGroupId(request.getGroupId())
                                                                             .setTerm(request.getCurrentTerm());
        rescheduleElection();
        ElectionStore electionStore = raftGroup.localElectionStore();
        LogEntryStore logEntryStore = raftGroup.localLogEntryStore();
        long lastAppliedIndex = logEntryStore.lastAppliedIndex();

        //1. Reply false if term < currentTerm
        if (request.getCurrentTerm() < electionStore.currentTerm()) {
            return responseBuilder.setFailure(buildAppendEntryFailure(lastAppliedIndex))
                                  .build();
        }

        //2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
        if (!logEntryStore.contains(request.getPrevLogIndex(), request.getPrevLogTerm())) {
            return responseBuilder.setFailure(buildAppendEntryFailure(lastAppliedIndex))
                                  .build();
        }

        // Update the current term if leader is from greater term
        if (electionStore.currentTerm() < request.getCurrentTerm()) {
            electionStore.updateCurrentTerm(request.getCurrentTerm());
        }

        //4. Append any new entries not already in the log
        for (Entry entry : request.getEntriesList()) {
            try {
                //3. If an existing entry conflicts with a new one (same index but different terms), delete the existing
                // entry and all that follow it
                raftGroup.localLogEntryStore().appendEntry(entry);
                lastAppliedIndex = logEntryStore.lastAppliedIndex();
            } catch (IncorrectTermException e) {
                return responseBuilder.setFailure(buildAppendEntryFailure(lastAppliedIndex))
                                      .build();
            } catch (IOException e) {
                stop();
                return responseBuilder.setFailure(buildAppendEntryFailure(lastAppliedIndex))
                                      .build();
            }
        }

        //5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
        if (request.getCommitIndex() > logEntryStore.commitIndex()) {
            logEntryStore.markCommitted(min(request.getCommitIndex(),
                                            request.getEntries(request.getEntriesCount() - 1).getIndex()));
        }

        return responseBuilder.setSuccess(buildAppendEntrySuccess(lastAppliedIndex))
                              .build();
    }

    private AppendEntryFailure buildAppendEntryFailure(long lastAppliedIndex) {
        return AppendEntryFailure.newBuilder()
                                 .setLastAppliedIndex(lastAppliedIndex)
                                 // TODO: 11/14/2018 lastAppliedEventSequence???
                                 .build();
    }

    private AppendEntrySuccess buildAppendEntrySuccess(long lastAppliedIndex) {
        return AppendEntrySuccess.newBuilder()
                                 .setLastLogIndex(lastAppliedIndex)
                                 .build();
    }

    @Override
    public RequestVoteResponse requestVote(RequestVoteRequest request) {
        throw new NotImplementedException();
    }

    @Override
    public InstallSnapshotResponse installSnapshot(InstallSnapshotRequest request) {
        throw new NotImplementedException();
    }

    public synchronized void initialize() {
        scheduleNewElection();
    }

    private void onExit() {
        cancelCurrentElection();
        scheduledExecutorService.shutdown();
    }

    private void cancelCurrentElection() {
        if (scheduledElection != null) {
            scheduledElection.cancel(true);
        }
    }

    private void scheduleNewElection() {
        scheduledElection = scheduledExecutorService.schedule(() -> {
            onExit();
//            transitionHandler.accept(new CandidateState());
            // TODO: make ThreadLocalRandom injectable
        }, ThreadLocalRandom.current().nextLong(electionTimeoutMin, electionTimeoutMax + 1), TimeUnit.MILLISECONDS);
    }

    private void rescheduleElection() {
        cancelCurrentElection();
        scheduleNewElection();
    }

    public static class Builder {

        private long electionTimeoutMin = 150;
        private long electionTimeoutMax = 300;
        private RaftGroup raftGroup;
        private Consumer<MembershipState> transitionHandler;
        private ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

        public Builder electionTimeoutMin(long electionTimeoutMin) {
            this.electionTimeoutMin = electionTimeoutMin;
            return this;
        }

        public Builder electionTimeoutMax(long electionTimeoutMax) {
            this.electionTimeoutMax = electionTimeoutMax;
            return this;
        }

        public Builder raftGroup(RaftGroup raftGroup) {
            this.raftGroup = raftGroup;
            return this;
        }

        public Builder transitionHandler(Consumer<MembershipState> transitionHandler) {
            this.transitionHandler = transitionHandler;
            return this;
        }

        public Builder scheduledExecutorService(ScheduledExecutorService scheduledExecutorService) {
            this.scheduledExecutorService = scheduledExecutorService;
            return this;
        }

        protected void validate() {
            //todo make assertions
        }

        public FollowerState build() {
            return new FollowerState(this);
        }
    }
}
