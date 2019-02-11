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

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static java.util.Collections.emptyList;

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

    default boolean isIdle() {
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

    default void forceStepDown() {

    }

    default List<Node> currentGroupMembers(){
        return emptyList();
    }
}
