package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.grpc.cluster.*;

public interface MembershipState {

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
}
