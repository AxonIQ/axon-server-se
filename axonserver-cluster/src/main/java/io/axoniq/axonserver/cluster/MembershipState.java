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

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

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

    default void forceStartElection() {

    }

    default List<Node> currentGroupMembers(){
        return emptyList();
    }

    default Iterator<ReplicatorPeerStatus> replicatorPeers() {
        throw new UnsupportedOperationException("Operation only supported in leader state");
    }

    default CurrentConfiguration currentConfiguration() {
        return new CurrentConfiguration() {
            @Override
            public List<Node> groupMembers() {
                return currentGroupMembers();
            }

            @Override
            public boolean isUncommitted() {
                return false;
            }
        };
    }

    /**
     * Starts process to transfer leadership from this node to another. No-op when the current node is not a leader.
     * @return completable future that completes when other node is up to date.
     */
    default CompletableFuture<Void> transferLeadership() {
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Handles a pre-vote request from another node in cluster.
     * <p>
     * Returns true if current state is Candidate, Pre-Vote, or Follower State (with time out on message from leader).
     * Returns false otherwise. Adds flag goAway if current state is leader and node from request is unknown.
     *
     * @param request the vote request
     * @return pre-vote allowed
     */
    RequestVoteResponse requestPreVote(RequestVoteRequest request);

    /**
     * Checks health of the node based on the current state.
     *
     * @param statusConsumer consumer to provide status messages to
     * @return true if this node considers itself healthy
     */
    default boolean health(BiConsumer<String, String> statusConsumer) {
        return true;
    }
}
