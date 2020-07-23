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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

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
    private final AtomicReference<String> leader = new AtomicReference<>();
    private final AtomicLong currentTerm = new AtomicLong();
    private final RaftGroup raftGroup;

    public FatalState(RaftResponseFactory raftResponseFactory, RaftGroup raftGroup) {
        this.raftResponseFactory = raftResponseFactory;
        this.raftGroup = raftGroup;
    }

    @Override
    public void start() {
    }

    @Override
    public boolean isIdle() {
        return true;
    }

    /**
     * Always rejects votes as this node should not influence leader election.
     *
     * @param request the vote request
     * @return rejected vote
     */
    @Override
    public RequestVoteResponse requestPreVote(RequestVoteRequest request) {
        return raftResponseFactory.voteRejected(request.getRequestId());
    }

    /**
     * Handles request from leader. Checks if this request contains a new leader, to update the current leader known on
     * this node.
     *
     * @param request the append entries request
     * @return response indicating that this node is in fatal state
     */
    @Override
    public AppendEntriesResponse appendEntries(AppendEntriesRequest request) {
        if (request.getTerm() > currentTerm.get()) {
            currentTerm.set(request.getTerm());
            if (!request.getLeaderId().equals(leader.get())) {
                leader.set(request.getLeaderId());
                raftGroup.localNode().notifyNewLeader(request.getLeaderId());
            }
        }
        return raftResponseFactory.appendEntriesFailure(request.getRequestId(), "In fatal state",
                                                        request.getSupportsReplicationGroups(), true);
    }

    /**
     * Always rejects votes as this node should not influence leader election.
     *
     * @param request the vote request
     * @return rejected vote
     */
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

    @Override
    public String getLeader() {
        return leader.get();
    }

    @Override
    public List<Node> currentGroupMembers() {
        return raftGroup.raftConfiguration().groupMembers();
    }

    @Override
    public boolean health(BiConsumer<String, String> statusConsumer) {
        statusConsumer.accept(raftGroup.raftConfiguration().groupId() + ".status", "FATAL");
        return false;
    }
}
