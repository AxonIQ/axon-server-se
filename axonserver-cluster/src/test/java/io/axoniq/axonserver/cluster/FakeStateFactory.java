package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.grpc.cluster.AppendEntriesRequest;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesResponse;
import io.axoniq.axonserver.grpc.cluster.AppendEntrySuccess;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotRequest;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotResponse;
import io.axoniq.axonserver.grpc.cluster.RequestVoteRequest;
import io.axoniq.axonserver.grpc.cluster.RequestVoteResponse;

/**
 * @author Sara Pellegrini
 * @since
 */
public class FakeStateFactory implements MembershipStateFactory {

    private FakeState lastStateCreated;

    @Override
    public MembershipState idleState(String nodeId) {
        return new FakeState("idle");
    }

    @Override
    public MembershipState leaderState() {
        lastStateCreated = new FakeState("leader");
        return lastStateCreated;
    }

    @Override
    public MembershipState followerState() {
        lastStateCreated = new FakeState("follower");
        return lastStateCreated;
    }

    @Override
    public MembershipState candidateState() {
        lastStateCreated = new FakeState("candidate");
        return lastStateCreated;
    }

    @Override
    public MembershipState preVoteState() {
        lastStateCreated = new FakeState("prevote");
        return lastStateCreated;
    }

    @Override
    public MembershipState removedState() {
        lastStateCreated = new FakeState("removed");
        return lastStateCreated;
    }

    public class FakeState implements MembershipState {

        private final String name;
        private String lastMethodCalled;

        public FakeState(String name) {
            this.name = name;
        }

        @Override
        public AppendEntriesResponse appendEntries(AppendEntriesRequest request) {
            lastMethodCalled = "appendEntries";
            return AppendEntriesResponse.newBuilder().setSuccess(
                    AppendEntrySuccess.newBuilder()
                                      .build()
            ).build();
        }

        @Override
        public RequestVoteResponse requestVote(RequestVoteRequest request) {
            lastMethodCalled = "requestVote";
            return null;
        }

        @Override
        public RequestVoteResponse requestPreVote(RequestVoteRequest request) {
            lastMethodCalled = "requestPreVote";
            return null;
        }

        @Override
        public InstallSnapshotResponse installSnapshot(InstallSnapshotRequest request) {
            lastMethodCalled = "installSnapshot";
            return null;
        }

        public String name() {
            return name;
        }

        public String lastMethodCalled() {
            return lastMethodCalled;
        }
    }
}
