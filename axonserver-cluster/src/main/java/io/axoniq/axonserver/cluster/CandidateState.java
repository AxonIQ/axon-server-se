package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.grpc.cluster.AppendEntriesRequest;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesResponse;
import io.axoniq.axonserver.grpc.cluster.AppendEntryFailure;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotRequest;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotResponse;
import io.axoniq.axonserver.grpc.cluster.RequestVoteRequest;
import io.axoniq.axonserver.grpc.cluster.RequestVoteResponse;

import java.util.Optional;
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

    private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    private final AtomicReference<ScheduledFuture<?>> currentElectionTimeoutTask = new AtomicReference<>();
    private final AtomicReference<Election> currentElection = new AtomicReference<>();

    public static class Builder extends AbstractMembershipState.Builder {

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

        public CandidateState build() {
            return new CandidateState(this);
        }
    }

    private CandidateState(Builder builder) {
        super(builder);
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
            FollowerState followerState = followerState();
            changeStateTo(followerState);
            return followerState.appendEntries(request);
        } else {
            return appendEntriesFailure();
        }
    }

    @Override
    public RequestVoteResponse requestVote(RequestVoteRequest request) {
        if (request.getTerm() > currentTerm()) {
            FollowerState followerState = followerState();
            changeStateTo(followerState);
            return followerState.requestVote(request);
        }

        return RequestVoteResponse.newBuilder()
                                  .setGroupId(groupId())
                                  .setTerm(currentTerm())
                                  .setVoteGranted(false)
                                  .build();
    }

    @Override
    public InstallSnapshotResponse installSnapshot(InstallSnapshotRequest request) {
        if (request.getTerm() > currentTerm()) {
            FollowerState followerState = followerState();
            changeStateTo(followerState);
            return followerState.installSnapshot(request);
        }
        return InstallSnapshotResponse.newBuilder()
                                      .setGroupId(groupId())
                                      .setTerm(currentTerm())
                                      .build();
    }

    private void resetElectionTimeout() {
        Optional.ofNullable(currentElectionTimeoutTask.get()).ifPresent(task -> task.cancel(true));
        long timeout = ThreadLocalRandom.current().nextLong(minElectionTimeout(), maxElectionTimeout());
        ScheduledFuture<?> newTimeoutTask = executorService.schedule(this::onElectionTimeout,
                                                                     timeout + 1,
                                                                     MILLISECONDS);
        currentElectionTimeoutTask.set(newTimeoutTask);
    }

    private void onElectionTimeout() {
        updateCurrentTerm(currentTerm() + 1);
        resetElectionTimeout();
        long raftGroupSize = raftGroup().raftConfiguration().groupMembers().size();
        currentElection.set(new CandidateElection(raftGroupSize));
        currentElection.get().onVoteReceived(me(), true);
        markVotedFor(me());
        RequestVoteRequest request = RequestVoteRequest.newBuilder()
                                                       .setGroupId(groupId())
                                                       .setCandidateId(me())
                                                       .setTerm(currentTerm())
                                                       .setLastLogIndex(lastLogIndex())
                                                       .setLastLogTerm(lastLogTerm())
                                                       .build();
        otherNodes().forEach(node -> requestVote(request, node));
    }

    private void requestVote(RequestVoteRequest request, RaftPeer node) {
        node.requestVote(request).thenAccept(response -> onVoteResponse(node.nodeId(), response));
    }

    private void onVoteResponse(String voter, RequestVoteResponse response) {
        if (response.getTerm() > currentTerm()) {
            changeStateTo(followerState());
            return;
        }
        if (response.getTerm() < currentTerm()) {
            return;
        }

        Election election = this.currentElection.get();
        election.onVoteReceived(voter, response.getVoteGranted());
        if (election.isWon()) {
            changeStateTo(leaderState());
        }
    }


    private FollowerState followerState() {
        return FollowerState.builder()
                            .raftGroup(raftGroup())
                            .transitionHandler(transitionHandler())
                            .build();
    }

    private LeaderState leaderState() {
        return LeaderState.builder()
                          .raftGroup(raftGroup())
                          .transitionHandler(transitionHandler())
                          .build();
    }
}
