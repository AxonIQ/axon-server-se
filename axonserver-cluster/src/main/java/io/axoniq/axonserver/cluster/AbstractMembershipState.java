package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.election.ElectionStore;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesResponse;
import io.axoniq.axonserver.grpc.cluster.AppendEntryFailure;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotFailure;
import io.axoniq.axonserver.grpc.cluster.InstallSnapshotResponse;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.cluster.RequestVoteResponse;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Sara Pellegrini
 * @since 4.0
 */
public abstract class AbstractMembershipState implements MembershipState {

    private final RaftGroup raftGroup;
    private final Consumer<MembershipState> transitionHandler;
    private final MembershipStateFactory stateFactory;
    private final Supplier<Long> currentTimeSupplier;
    private final List<RaftPeer> otherNodes;

    protected AbstractMembershipState(Builder builder) {
        builder.validate();
        this.raftGroup = builder.raftGroup;
        this.transitionHandler = builder.transitionHandler;
        this.stateFactory = builder.stateFactory;
        this.currentTimeSupplier = builder.currentTimeSupplier;
        this.otherNodes =  raftGroup.raftConfiguration().groupMembers().stream()
                    .map(Node::getNodeId)
                    .filter(id -> !id.equals(me()))
                    .map(raftGroup::peer).collect(Collectors.toList());

    }



    public static abstract class Builder<B extends Builder<B>> {

        private RaftGroup raftGroup;
        private Consumer<MembershipState> transitionHandler;
        private MembershipStateFactory stateFactory;
        private Supplier<Long> currentTimeSupplier = System::currentTimeMillis;

        public B raftGroup(RaftGroup raftGroup) {
            this.raftGroup = raftGroup;
            return self();
        }

        public B transitionHandler(Consumer<MembershipState> transitionHandler) {
            this.transitionHandler = transitionHandler;
            return self();
        }

        public B stateFactory(MembershipStateFactory stateFactory) {
            this.stateFactory = stateFactory;
            return self();
        }

        public B currentTimeSupplier(Supplier<Long> currentTimeSupplier) {
            this.currentTimeSupplier = currentTimeSupplier;
            return self();
        }

        protected void validate() {
            if (raftGroup == null) {
                throw new IllegalStateException("The RAFT group must be provided");
            }
            if (transitionHandler == null) {
                throw new IllegalStateException("The transitionHandler must be provided");
            }
            if (stateFactory == null) {
                throw new IllegalStateException("The stateFactory must be provided");
            }
        }

        @SuppressWarnings("unchecked")
        private final B self() {
            return (B) this;
        }

        abstract MembershipState build();
    }

    protected String votedFor() {
        return raftGroup.localElectionStore().votedFor();
    }

    protected void markVotedFor(String candidateId) {
        raftGroup.localElectionStore().markVotedFor(candidateId);
    }

    protected long lastAppliedIndex() {
        return raftGroup.localLogEntryStore().lastAppliedIndex();
    }

    protected long lastLogTerm() {
        return raftGroup.localLogEntryStore().lastLogTerm();
    }

    protected long lastLogIndex() {
        return raftGroup.localLogEntryStore().lastLogIndex();
    }

    protected long currentTerm() {
        return raftGroup.localElectionStore().currentTerm();
    }

    String me() {
        return raftGroup.localNode().nodeId();
    }

    protected RaftGroup raftGroup() {
        return raftGroup;
    }


    protected Consumer<MembershipState> transitionHandler() {
        return transitionHandler;
    }

    public MembershipStateFactory stateFactory() {
        return stateFactory;
    }

    protected long lastAppliedEventSequence() {
        return raftGroup.lastAppliedEventSequence();
    }

    protected void changeStateTo(MembershipState newState) {
        transitionHandler.accept(newState);
    }

    protected void updateCurrentTerm(long term) {
        if (term > currentTerm()) {
            ElectionStore electionStore = raftGroup.localElectionStore();
            electionStore.updateCurrentTerm(term);
            electionStore.markVotedFor(null);
        }
    }

    protected long maxElectionTimeout() {
        return raftGroup.raftConfiguration().maxElectionTimeout();
    }

    protected long minElectionTimeout() {
        return raftGroup.raftConfiguration().minElectionTimeout();
    }

    protected String groupId() {
        return raftGroup().raftConfiguration().groupId();
    }

    protected Stream<RaftPeer> otherNodesStream() {
        return otherNodes.stream();
    }

    protected Iterable<RaftPeer> otherNodes() {
        return otherNodes;
    }

    protected long otherNodesCount() {
        return otherNodes.size();
    }

    protected long currentTimeMillis() {
        return currentTimeSupplier.get();
    }

    protected AppendEntriesResponse appendEntriesFailure() {
        AppendEntryFailure failure = AppendEntryFailure.newBuilder()
                                                       .setLastAppliedIndex(lastAppliedIndex())
                                                       .setLastAppliedEventSequence(lastAppliedEventSequence())
                                                       .build();
        return AppendEntriesResponse.newBuilder()
                                    .setGroupId(groupId())
                                    .setTerm(currentTerm())
                                    .setFailure(failure)
                                    .build();
    }

    protected InstallSnapshotResponse installSnapshotFailure() {
        return InstallSnapshotResponse.newBuilder()
                                      .setGroupId(groupId())
                                      .setTerm(currentTerm())
                                      .setFailure(InstallSnapshotFailure.newBuilder().build())
                                      .build();
    }

    protected RequestVoteResponse requestVoteResponse(boolean voteGranted) {
        return RequestVoteResponse.newBuilder()
                                  .setGroupId(groupId())
                                  .setVoteGranted(voteGranted)
                                  .setTerm(currentTerm())
                                  .build();
    }
}
