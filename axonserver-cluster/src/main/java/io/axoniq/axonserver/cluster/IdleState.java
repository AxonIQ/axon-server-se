package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.grpc.cluster.*;

import java.util.function.Consumer;

public class IdleState implements MembershipState {

    private final RaftGroup raftGroup;
    private final Consumer<MembershipState> transitionHandler;

    public IdleState(RaftGroup raftGroup, Consumer<MembershipState> transitionHandler) {
        this.raftGroup = raftGroup;
        this.transitionHandler = transitionHandler;
    }

    @Override
    public void start() {
        FollowerState followerState = FollowerState.builder()
                                                   .raftGroup(raftGroup)
                                                   .transitionHandler(transitionHandler)
                                                   .build();
        transitionHandler.accept(followerState);
        followerState.initialize();
    }

    @Override
    public AppendEntriesResponse appendEntries(AppendEntriesRequest request) {
        throw new IllegalStateException();
    }

    @Override
    public RequestVoteResponse requestVote(RequestVoteRequest request) {
        throw new IllegalStateException();
    }

    @Override
    public InstallSnapshotResponse installSnapshot(InstallSnapshotRequest request) {
        throw new IllegalStateException();
    }

    @Override
    public void stop() {

    }
}
