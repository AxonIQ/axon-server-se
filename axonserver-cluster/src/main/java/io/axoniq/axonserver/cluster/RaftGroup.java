package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.election.ElectionStore;
import io.axoniq.axonserver.cluster.replication.LogEntryStore;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesRequest;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesResponse;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotRequest;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotResponse;
import io.axoniq.axonserver.grpc.cluster.RequestVoteRequest;
import io.axoniq.axonserver.grpc.cluster.RequestVoteResponse;

import java.util.function.Function;

public interface RaftGroup {

    Registration onAppendEntries(Function<AppendEntriesRequest, AppendEntriesResponse> handler);

    Registration onInstallSnapshot(Function<InstallSnapshotRequest, InstallSnapshotResponse> handler);

    Registration onRequestVote(Function<RequestVoteRequest, RequestVoteResponse> handler);

    LogEntryStore localLogEntryStore();

    ElectionStore localElectionStore();

    RaftConfiguration raftConfiguration();

    default long lastAppliedEventSequence() {
        return -1L;
    }

    RaftPeer peer(String nodeId);

    RaftNode localNode();

    default void connect() {
        localNode().start();
    }
}
