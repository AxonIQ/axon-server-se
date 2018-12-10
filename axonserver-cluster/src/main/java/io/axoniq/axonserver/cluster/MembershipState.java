package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.configuration.ClusterConfiguration;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesRequest;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesResponse;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotRequest;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotResponse;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.cluster.RequestVoteRequest;
import io.axoniq.axonserver.grpc.cluster.RequestVoteResponse;

import java.util.concurrent.CompletableFuture;

public interface MembershipState extends ClusterConfiguration{

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

    default void applied(Entry e) {}

    default String getLeader() {
        return null;
    }

    default CompletableFuture<Void> registerNode(Node node) {
        CompletableFuture<Void> cf = new CompletableFuture<>();
        cf.completeExceptionally(new UnsupportedOperationException());
        return cf;
    }

    default CompletableFuture<Void> unregisterNode(String nodeId) {
        CompletableFuture<Void> cf = new CompletableFuture<>();
        cf.completeExceptionally(new UnsupportedOperationException());
        return cf;
    }
}
