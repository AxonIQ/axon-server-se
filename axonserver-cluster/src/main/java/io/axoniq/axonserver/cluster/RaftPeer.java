package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.util.RoleUtils;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesRequest;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesResponse;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotRequest;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotResponse;
import io.axoniq.axonserver.grpc.cluster.RequestVoteRequest;
import io.axoniq.axonserver.grpc.cluster.RequestVoteResponse;
import io.axoniq.axonserver.grpc.cluster.Role;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public interface RaftPeer {

    CompletableFuture<RequestVoteResponse> requestVote(RequestVoteRequest request);

    CompletableFuture<RequestVoteResponse> requestPreVote(RequestVoteRequest request);

    void appendEntries(AppendEntriesRequest request);

    void installSnapshot(InstallSnapshotRequest request);

    Registration registerAppendEntriesResponseListener(Consumer<AppendEntriesResponse> listener);

    Registration registerInstallSnapshotResponseListener(Consumer<InstallSnapshotResponse> listener);

    String nodeId();

    String nodeName();

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

    /**
     * Sends a message to the peer to timeout immediately, causing it to start a new election.
     */
    void sendTimeoutNow();

    /**
     * Retrieves the role of the peer in the raft group.
     *
     * @return the role of the peer in the raft group
     */
    Role role();

    /**
     * Checks if peer is primary node.
     *
     * @return true if peer is primary node
     */
    boolean primaryNode();

    /**
     * Checks if peer is involved in leader elections and transactions.
     *
     * @return true if peer is involved in leader elections and transactions
     */
    boolean votingNode();

    /**
     * Checks if the node is configured as an event store. Node is primary or backup (active or passive) node.
     *
     * @return true if the node is storing events
     */
    default boolean eventStore() {
        return RoleUtils.hasStorage(role());
    }
}
