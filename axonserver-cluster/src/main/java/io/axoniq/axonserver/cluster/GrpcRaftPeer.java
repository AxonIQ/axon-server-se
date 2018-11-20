package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.grpc.cluster.*;

import java.util.concurrent.CompletableFuture;

public class GrpcRaftPeer implements RaftPeer {
    public final Node node;

    public GrpcRaftPeer(Node node) {
        this.node = node;
    }

    @Override
    public CompletableFuture<RequestVoteResponse> requestVote(RequestVoteRequest request) {

        return null;
    }

    @Override
    public CompletableFuture<AppendEntriesResponse> appendEntries(AppendEntriesRequest request) {
        return null;
    }

    @Override
    public CompletableFuture<InstallSnapshotResponse> installSnapshot(InstallSnapshotRequest request) {
        return null;
    }

    @Override
    public String nodeId() {
        return node.getNodeId();
    }
}
