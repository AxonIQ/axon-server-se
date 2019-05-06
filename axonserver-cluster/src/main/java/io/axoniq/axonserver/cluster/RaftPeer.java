package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.grpc.cluster.AppendEntriesRequest;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesResponse;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotRequest;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotResponse;
import io.axoniq.axonserver.grpc.cluster.RequestVoteRequest;
import io.axoniq.axonserver.grpc.cluster.RequestVoteResponse;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public interface RaftPeer {

    CompletableFuture<RequestVoteResponse> requestVote(RequestVoteRequest request);

    void appendEntries(AppendEntriesRequest request);

    void installSnapshot(InstallSnapshotRequest request);

    Registration registerAppendEntriesResponseListener(Consumer<AppendEntriesResponse> listener);

    Registration registerInstallSnapshotResponseListener(Consumer<InstallSnapshotResponse> listener);

    String nodeId();

    /**
     * Checks if connection is ready to send appendEntries requests.
     * @return true if connection is ready
     */
    default boolean isReadyForAppendEntries() {
        return true;
    }

    /**
     * Checks if connection is ready to send installSnapshot requests.
     * @return true if connection is ready
     */
    default boolean isReadyForSnapshot() {
        return true;
    }
}
