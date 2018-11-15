package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.grpc.cluster.AppendEntriesRequest;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesResponse;
import io.axoniq.axonserver.grpc.cluster.AppendEntryFailure;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotRequest;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotResponse;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.cluster.RequestVoteRequest;
import io.axoniq.axonserver.grpc.cluster.RequestVoteResponse;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;

/**
 * @author Sara Pellegrini
 * @since 4.0
 */
public class CandidateState extends AbstractMembershipState {

    private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    private final AtomicReference<ScheduledFuture<?>> currentElectionTimeoutTask = new AtomicReference<>();
    private final Map<String, Boolean> receivedVotes = new ConcurrentHashMap<>();

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
            changeState(followerState);
            return followerState.appendEntries(request);
        } else {
            return AppendEntriesResponse.newBuilder()
                                        .setGroupId(request.getGroupId())
                                        .setTerm(currentTerm())
                                        .setFailure(AppendEntryFailure.newBuilder()
                                                                      .setLastAppliedIndex(lastLogAppliedIndex())
                                                                      .setLastAppliedEventSequence(lastAppliedEventSequence())
                                                                      .build())
                                        .build();
        }
    }

    @Override
    public RequestVoteResponse requestVote(RequestVoteRequest request) {
        if (request.getTerm() > currentTerm()) {
            FollowerState followerState = followerState();
            changeState(followerState);
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
            changeState(followerState);
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
        ScheduledFuture<?> newTimeoutTask = executorService.schedule(this::onElectionTimeout, timeout + 1, MILLISECONDS);
        currentElectionTimeoutTask.set(newTimeoutTask);
    }

    private void onElectionTimeout() {
        updateCurrentTerm(currentTerm() + 1);
        markVotedFor(me());
        resetElectionTimeout();
        RequestVoteRequest request = RequestVoteRequest.newBuilder()
                                                       .setGroupId(groupId())
                                                       .setCandidateId(me())
                                                       .setTerm(currentTerm())
                                                       .setLastLogIndex(lastLogIndex())
                                                       .setLastLogTerm(lastLogTerm())
                                                       .build();
        otherNodes().forEach(node -> requestVote(request, node));
    }

    Iterable<RaftPeer> otherNodes() {
        List<Node> nodes = raftGroup().raftConfiguration().groupMembers();
        return nodes.stream()
                    .map(Node::getNodeId)
                    .filter(id -> !id.equals(me()))
                    .map(raftGroup()::peer)
                    .collect(toList());
    }

    private void requestVote(RequestVoteRequest request, RaftPeer node) {
        node.requestVote(request).thenAccept(response -> onVoteResponse(node.nodeId(), response));
    }

    private void onVoteResponse(String voter, RequestVoteResponse response) {
        if (response.getTerm() > currentTerm()) {
            changeState(followerState());
            return;
        }
        this.receivedVotes.put(voter, response.getVoteGranted());
        if (electionWon()) {
            changeState(leaderState());
        }
    }

    private boolean electionWon() {
        long voteGranted = receivedVotes.values().stream().filter(granted -> granted).count();
        return voteGranted >= minMajority();
    }

    private long minMajority() {
        int size = raftGroup().raftConfiguration().groupMembers().size();
        return (size / 2) + (size % 2 == 0 ? 0L : 1L);
    }

    private FollowerState followerState(){
        return FollowerState.builder()
                            .raftGroup(raftGroup())
                            .transitionHandler(transitionHandler())
                            .build();
    }

    private LeaderState leaderState(){
        return LeaderState.builder().build();
    }
}
