package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.configuration.ClusterConfiguration;
import io.axoniq.axonserver.cluster.configuration.IdleConfiguration;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesRequest;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesResponse;
import io.axoniq.axonserver.grpc.cluster.ConfigChangeResult;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotRequest;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotResponse;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.cluster.RequestVoteRequest;
import io.axoniq.axonserver.grpc.cluster.RequestVoteResponse;

import java.util.concurrent.CompletableFuture;

/**
 * Final state for a raft node when it has failed with an unrecoverable error. Responds to append entries requests with
 * fatal indication, so that leader can stop sending updates to this node for some time.
 *
 * @author Marc Gathier
 * @since 4.2
 */
public class FatalState implements MembershipState {

    private final ClusterConfiguration clusterConfiguration = new IdleConfiguration();
    private final RaftResponseFactory raftResponseFactory;

    public FatalState(RaftResponseFactory raftResponseFactory) {
        this.raftResponseFactory = raftResponseFactory;
    }

    @Override
    public void start() {
    }

    @Override
    public boolean isIdle() {
        return true;
    }

    @Override
    public RequestVoteResponse requestPreVote(RequestVoteRequest request) {
        return raftResponseFactory.voteResponse(request.getRequestId(), true);
    }

    @Override
    public AppendEntriesResponse appendEntries(AppendEntriesRequest request) {
        return raftResponseFactory.appendEntriesFailure(request.getRequestId(), "In fatal state", true);
    }

    @Override
    public RequestVoteResponse requestVote(RequestVoteRequest request) {
        return raftResponseFactory.voteRejected(request.getRequestId());
    }

    @Override
    public InstallSnapshotResponse installSnapshot(InstallSnapshotRequest request) {
        return raftResponseFactory.installSnapshotFailure(request.getRequestId(), "In fatal state");
    }

    @Override
    public void stop() {

    }

    @Override
    public CompletableFuture<ConfigChangeResult> addServer(Node node) {
        return clusterConfiguration.addServer(node);
    }

    @Override
    public CompletableFuture<ConfigChangeResult> removeServer(String nodeId) {
        return clusterConfiguration.removeServer(nodeId);
    }
}
