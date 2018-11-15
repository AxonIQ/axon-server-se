package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.grpc.cluster.AppendEntriesRequest;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesResponse;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotRequest;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotResponse;
import io.axoniq.axonserver.grpc.cluster.RequestVoteRequest;
import io.axoniq.axonserver.grpc.cluster.RequestVoteResponse;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.function.Consumer;

/**
 * @author Sara Pellegrini
 * @since 4.0
 */
public class LeaderState implements MembershipState {

    private final RaftGroup raftGroup;
    private final Consumer<MembershipState> transitionHandler;

    public LeaderState(RaftGroup raftGroup,
                       Consumer<MembershipState> transitionHandler) {
        this.raftGroup = raftGroup;
        this.transitionHandler = transitionHandler;
    }

    @Override
    public void stop() {
        throw new NotImplementedException();
    }

    @Override
    public AppendEntriesResponse appendEntries(AppendEntriesRequest request) {
        throw new NotImplementedException();
    }

    @Override
    public RequestVoteResponse requestVote(RequestVoteRequest request) {
        throw new NotImplementedException();
    }

    @Override
    public InstallSnapshotResponse installSnapshot(InstallSnapshotRequest request) {
        throw new NotImplementedException();
    }
}
