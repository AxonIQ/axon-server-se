package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.election.ElectionStore;
import io.axoniq.axonserver.cluster.replication.LogEntryStore;
import io.axoniq.axonserver.grpc.cluster.*;

import java.util.function.Function;
import java.util.function.Supplier;

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
