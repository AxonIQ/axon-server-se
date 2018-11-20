package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.grpc.cluster.*;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public interface RaftPeer {

    CompletableFuture<RequestVoteResponse> requestVote(RequestVoteRequest request);

    CompletableFuture<AppendEntriesResponse> appendEntries(AppendEntriesRequest request);

    CompletableFuture<InstallSnapshotResponse> installSnapshot(InstallSnapshotRequest request);

    String nodeId();
}
