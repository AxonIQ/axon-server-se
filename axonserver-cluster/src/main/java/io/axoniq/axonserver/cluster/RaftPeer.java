package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.grpc.cluster.*;

import java.util.concurrent.CompletableFuture;

public interface RaftPeer {

    CompletableFuture<AppendEntriesResponse> appendEntries(AppendEntriesRequest request);

    CompletableFuture<InstallSnapshotResponse> installSnapshot(InstallSnapshotRequest request);

    CompletableFuture<RequestVoteResponse> requestVote(RequestVoteRequest request);

    String nodeId();

}
