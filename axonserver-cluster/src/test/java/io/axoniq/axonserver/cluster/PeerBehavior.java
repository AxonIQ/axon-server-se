package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.grpc.cluster.*;

public interface PeerBehavior {

    AppendEntriesResponse appendEntries(AppendEntriesRequest request);

    InstallSnapshotResponse installSnapshot(InstallSnapshotRequest request);

    RequestVoteResponse requestVote(RequestVoteRequest request);

}
