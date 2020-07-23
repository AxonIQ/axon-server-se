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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

public class IdleState implements MembershipState {

    private final Logger logger = LoggerFactory.getLogger(IdleState.class);
    private final String nodeId;
    private final ClusterConfiguration clusterConfiguration = new IdleConfiguration();
    private final RaftResponseFactory raftResponseFactory;

    public IdleState(String nodeId, RaftResponseFactory raftResponseFactory) {
        this.nodeId = nodeId;
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
        logger.info("Ignored AppendEntriesRequest with requestId {}: I ({}) am in idle state.",
                    request.getRequestId(),
                    nodeId);
        return raftResponseFactory.appendEntriesFailure(request.getRequestId(),
                                                        request.getSupportsReplicationGroups(),
                                                        "In idle state.");
    }

    @Override
    public RequestVoteResponse requestVote(RequestVoteRequest request) {
        logger.info("Ignored RequestVoteRequest with requestId {}: I ({}) am in idle state.",
                    request.getRequestId(),
                    nodeId);
        return raftResponseFactory.voteRejected(request.getRequestId());
    }

    @Override
    public InstallSnapshotResponse installSnapshot(InstallSnapshotRequest request) {
        logger.info("Ignored InstallSnapshotRequest with requestId {}: I ({}) am in idle state.",
                    request.getRequestId(),
                    nodeId);
        return raftResponseFactory.installSnapshotFailure(request.getRequestId(), "In idle state.");
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
