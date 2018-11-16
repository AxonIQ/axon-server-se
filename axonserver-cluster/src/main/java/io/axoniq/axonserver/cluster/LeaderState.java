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
public class LeaderState extends AbstractMembershipState {

    protected static class Builder extends AbstractMembershipState.Builder<Builder> {

//        @Override
//        public Builder raftGroup(RaftGroup raftGroup) {
//            super.raftGroup(raftGroup);
//            return this;
//        }
//
//        @Override
//        public Builder transitionHandler(Consumer<MembershipState> transitionHandler) {
//            super.transitionHandler(transitionHandler);
//            return this;
//        }
//
//        @Override
//        public Builder stateFactory(MembershipStateFactory stateFactory) {
//            super.stateFactory(stateFactory);
//            return this;
//        }

        public LeaderState build(){
            return new LeaderState(this);
        }

    }

    private LeaderState(Builder builder) {
        super(builder);
    }

    public static Builder builder(){
        return new Builder();
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
