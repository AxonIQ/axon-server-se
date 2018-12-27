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

public class IdleState implements MembershipState {

    private final String nodeId;
    private final ClusterConfiguration clusterConfiguration = new IdleConfiguration();

    public IdleState(String nodeId) {
        this.nodeId = nodeId;
    }

    @Override
    public void start() {

    }

    @Override
    public boolean isIdle() {
        return true;
    }

    @Override
    public AppendEntriesResponse appendEntries(AppendEntriesRequest request) {
        throw new IllegalStateException(nodeId + " : in idle state");
    }

    @Override
    public RequestVoteResponse requestVote(RequestVoteRequest request) {
        throw new IllegalStateException(nodeId + " : in idle state");
    }

    @Override
    public InstallSnapshotResponse installSnapshot(InstallSnapshotRequest request) {
        throw new IllegalStateException(nodeId + " : in idle state");
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
