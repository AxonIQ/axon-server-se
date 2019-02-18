package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.grpc.cluster.AppendEntriesRequest;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesResponse;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotRequest;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotResponse;
import io.axoniq.axonserver.grpc.cluster.RequestVoteRequest;
import io.axoniq.axonserver.grpc.cluster.RequestVoteResponse;

public class RemovedState implements MembershipState {

    private final String nodeId;
    private final RaftGroup raftGroup;

    protected static class Builder  {
        private RaftGroup raftGroup;

        public RemovedState build() {
            return new RemovedState(this);
        }

        public Builder raftGroup(RaftGroup raftGroup) {
            this.raftGroup = raftGroup;
            return this;
        }
    }


    public static RemovedState.Builder builder() {
        return new RemovedState.Builder();
    }

    private RemovedState(RemovedState.Builder builder) {
        raftGroup = builder.raftGroup;
        nodeId = raftGroup.localNode().nodeId();
    }

    @Override
    public void start() {
        raftGroup.delete();

    }

    @Override
    public boolean isIdle() {
        return true;
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
