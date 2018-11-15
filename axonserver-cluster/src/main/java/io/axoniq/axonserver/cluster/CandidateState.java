package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.grpc.cluster.AppendEntriesRequest;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesResponse;
import io.axoniq.axonserver.grpc.cluster.AppendEntryFailure;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotRequest;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotResponse;
import io.axoniq.axonserver.grpc.cluster.RequestVoteRequest;
import io.axoniq.axonserver.grpc.cluster.RequestVoteResponse;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * @author Sara Pellegrini
 * @since 4.0
 */
public class CandidateState extends AbstractMembershipState {

    private final RaftGroup raftGroup;
    private final Consumer<MembershipState> transitionHandler;
    private final long minElectionTimeout;
    private final long maxElectionTimeout;
    private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    private final AtomicReference<ScheduledFuture<?>> currentElectionTimeoutTask = new AtomicReference<>();

    public static class Builder extends AbstractMembershipState.Builder {

        private int minElectionTimeout = 150;
        private int maxElectionTimeout = 300;
        private Consumer<MembershipState> transitionHandler;

        public Builder minElectionTimeout(int minElectionTimeout) {
            this.minElectionTimeout = minElectionTimeout;
            return this;
        }

        public Builder maxElectionTimeout(int maxElectionTimeout) {
            this.maxElectionTimeout = maxElectionTimeout;
            return this;
        }

        public Builder transitionHandler(Consumer<MembershipState> transitionHandler) {
            this.transitionHandler = transitionHandler;
            return this;
        }

        protected void validate() {
            super.validate();
        }

        public CandidateState build() {
            return new CandidateState(this);
        }
    }

    private CandidateState(Builder builder) {
        super(builder);
        this.raftGroup = builder.raftGroup;
        this.transitionHandler = builder.transitionHandler;
        this.maxElectionTimeout = builder.maxElectionTimeout;
        this.minElectionTimeout = builder.minElectionTimeout;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void start() {
        onElectionTimeout();
    }

    @Override
    public void stop() {
        executorService.shutdown();
    }

    @Override
    public AppendEntriesResponse appendEntries(AppendEntriesRequest request) {
        if (request.getTerm() >= currentTerm()) {
            updateCurrentTerm(request.getTerm()); //TODO check if done in follower
            FollowerState followerState = new FollowerState(raftGroup, transitionHandler);
            transitionHandler.accept(followerState);
            followerState.initialize();
            stop();
            return followerState.appendEntries(request);
        } else {
            return AppendEntriesResponse.newBuilder()
                                        .setGroupId(request.getGroupId())
                                        .setTerm(currentTerm())
                                        .setFailure(AppendEntryFailure.newBuilder()
                                                                      .setLastAppliedIndex(lastLogAppliedIndex())
//                                                                      .setLastAppliedEventSequence() //TODO
                                                                      .build())
                                        .build();
        }
    }

    @Override
    public RequestVoteResponse requestVote(RequestVoteRequest request) {
        if (request.getTerm() > currentTerm()) {
            updateCurrentTerm(request.getTerm()); //TODO check if done in follower
            FollowerState followerState = new FollowerState(raftGroup, transitionHandler);
            transitionHandler.accept(followerState);
            followerState.initialize();
            return followerState.requestVote(request);
        }

        return RequestVoteResponse.newBuilder()
                                  .setGroupId(request.getGroupId())
                                  .setTerm(currentTerm())
                                  .setVoteGranted(voteGrantedFor(request))
                                  .build();
    }

    @Override
    public InstallSnapshotResponse installSnapshot(InstallSnapshotRequest request) {
        if (request.getTerm() > currentTerm()) {
            updateCurrentTerm(request.getTerm()); //TODO check if done in follower
            FollowerState followerState = new FollowerState(raftGroup, transitionHandler);
            transitionHandler.accept(followerState);
            followerState.initialize();
            return followerState.installSnapshot(request);
        }
        return InstallSnapshotResponse.newBuilder()
                                      .setGroupId(request.getGroupId())
                                      .setTerm(currentTerm())
                                      .build();
    }

    private void resetElectionTimeout() {
        Optional.ofNullable(currentElectionTimeoutTask.get()).ifPresent(task -> task.cancel(true));
        long timeout = ThreadLocalRandom.current().nextLong(minElectionTimeout, maxElectionTimeout);
        ScheduledFuture<?> newTimeoutTask = executorService.schedule(this::onElectionTimeout, timeout, MILLISECONDS);
        currentElectionTimeoutTask.set(newTimeoutTask);
    }

    private void onElectionTimeout() {
        updateCurrentTerm(currentTerm() + 1);
        markVotedFor(me());
        resetElectionTimeout();
        CompletableFuture<ElectionResult> election = raftGroup.startElection();
        election.thenAccept(this::onElectionResult);
    }

    private void onElectionResult(ElectionResult result) {
        if (result.electedLeader()) {
            LeaderState leaderState = new LeaderState(raftGroup, transitionHandler);
            stop();
            transitionHandler.accept(leaderState);
            leaderState.start();
        }
    }
}
