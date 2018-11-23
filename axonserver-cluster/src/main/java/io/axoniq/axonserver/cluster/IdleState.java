package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.grpc.cluster.AppendEntriesRequest;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesResponse;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotRequest;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotResponse;
import io.axoniq.axonserver.grpc.cluster.RequestVoteRequest;
import io.axoniq.axonserver.grpc.cluster.RequestVoteResponse;

public class IdleState implements MembershipState {

    private final String nodeId;

    public IdleState(String nodeId) {
        this.nodeId = nodeId;
    }

    @Override
    public void start() {

    }

    @Override
    public AppendEntriesResponse appendEntries(AppendEntriesRequest request) {
        throw new IllegalStateException(nodeId + " : in idle state");
    }

    @Override
    public RequestVoteResponse requestVote(RequestVoteRequest request) {
        throw new IllegalStateException(nodeId + " : in idle state");
    }

    @Override
    public InstallSnapshotResponse installSnapshot(InstallSnapshotRequest request) {
        throw new IllegalStateException(nodeId + " : in idle state");
    }

    @Override
    public void stop() {

    }
}
