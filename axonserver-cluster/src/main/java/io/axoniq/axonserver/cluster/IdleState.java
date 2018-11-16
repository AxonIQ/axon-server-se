package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.grpc.cluster.AppendEntriesRequest;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesResponse;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotRequest;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotResponse;
import io.axoniq.axonserver.grpc.cluster.RequestVoteRequest;
import io.axoniq.axonserver.grpc.cluster.RequestVoteResponse;

public class IdleState implements MembershipState {

    @Override
    public void start() {

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
