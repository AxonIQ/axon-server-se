package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.grpc.cluster.AppendEntriesRequest;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesResponse;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotRequest;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotResponse;
import io.axoniq.axonserver.grpc.cluster.RequestVoteRequest;
import io.axoniq.axonserver.grpc.cluster.RequestVoteResponse;

import java.io.IOException;
import java.util.function.Consumer;

public class FollowerState implements MembershipState {
    private final RaftGroup raftGroup;
    private final Consumer<MembershipState> transitionHandler;

    public FollowerState(RaftGroup raftGroup, Consumer<MembershipState> transitionHandler) {
        this.raftGroup = raftGroup;
        this.transitionHandler = transitionHandler;
    }

    @Override
    public synchronized void stop() {

    }

    @Override
    public AppendEntriesResponse appendEntries(AppendEntriesRequest request) {
        // TODO reset timers
        // TODO validate the request
        try {
            // TODO for each entry:
            raftGroup.localLogEntryStore().appendEntry(request.getEntriesList());
        } catch (IOException e) {
            // TODO Build failed response
        }
        // TODO: Return success response
        throw new UnsupportedOperationException();
    }

    @Override
    public RequestVoteResponse requestVote(RequestVoteRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public InstallSnapshotResponse installSnapshot(InstallSnapshotRequest request) {
        throw new UnsupportedOperationException();
    }

    public synchronized void initialize() {
        throw new UnsupportedOperationException();
    }
}
