package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.grpc.cluster.*;

import java.util.concurrent.CompletableFuture;

public interface MembershipState {

    default void stop() {
        // no op
    }

    default void start() {
        // no op
    }

    AppendEntriesResponse appendEntries(AppendEntriesRequest request);

    RequestVoteResponse requestVote(RequestVoteRequest request);

    InstallSnapshotResponse installSnapshot(InstallSnapshotRequest request);

    default boolean isLeader() {
        return false;
    }

    default CompletableFuture<Void> appendEntry(String entryType, byte[] entryData) {
        CompletableFuture<Void> cf = new CompletableFuture<>();
        cf.completeExceptionally(new UnsupportedOperationException());
        return cf;
    }
}
