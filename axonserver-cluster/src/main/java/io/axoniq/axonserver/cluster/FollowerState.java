package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.replication.IncorrectTermException;
import io.axoniq.axonserver.grpc.cluster.*;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

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
            raftGroup.localLogEntryStore().appendEntry(request.getEntries(0));
        } catch (IncorrectTermException e) {
            // TODO Build failed response
        } catch (IOException e) {
            // TODO Build failed response
        }
        // TODO: Return success response
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

    public synchronized void initialize() {
        throw new NotImplementedException();
    }
}
