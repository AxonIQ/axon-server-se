package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.election.ElectionStore;

import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * @author Sara Pellegrini
 * @since 4.0
 */
public abstract class AbstractMembershipState implements MembershipState {

    private final RaftGroup raftGroup;
    private final Consumer<MembershipState> transitionHandler;
    private final Supplier<Long> lastAppliedEventSequenceSupplier;

    protected AbstractMembershipState(Builder builder){
        builder.validate();
        this.raftGroup = builder.raftGroup;
        this.transitionHandler = builder.transitionHandler;
        this.lastAppliedEventSequenceSupplier = builder.lastAppliedEventSequenceSupplier;
    }

    public static Builder builder(){
        return new Builder();
    }

    public static class Builder {

        private RaftGroup raftGroup;
        private Consumer<MembershipState> transitionHandler;
        private Supplier<Long> lastAppliedEventSequenceSupplier = () -> -1L;

        public Builder raftGroup(RaftGroup raftGroup){
            this.raftGroup = raftGroup;
            return this;
        }

        public Builder transitionHandler(Consumer<MembershipState> transitionHandler) {
            this.transitionHandler = transitionHandler;
            return this;
        }

        public Builder lastAppliedEventSequenceSupplier(Supplier<Long> lastAppliedEventSequenceSupplier) {
            this.lastAppliedEventSequenceSupplier = lastAppliedEventSequenceSupplier;
            return this;
        }

        protected void validate(){
            if (raftGroup == null){
                throw new IllegalStateException("The RAFT group must be provided");
            }
            if (transitionHandler == null) {
                throw new IllegalStateException("The transitionHandler must be provided");
            }
        }

    }

    protected String votedFor() {
        return raftGroup.localElectionStore().votedFor();
    }

    protected void markVotedFor(String candidateId) {
        raftGroup.localElectionStore().markVotedFor(candidateId);
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

    protected RaftGroup raftGroup() {
        return raftGroup;
    }

    protected Supplier<Long> lastAppliedEventSequenceSupplier() {
        return lastAppliedEventSequenceSupplier;
    }

    protected void transition(MembershipState newState) {
        transitionHandler.accept(newState);
    }

    protected void updateCurrentTerm(long term){
        if (term > currentTerm()){
            ElectionStore electionStore = raftGroup.localElectionStore();
            electionStore.updateCurrentTerm(term);
            electionStore.markVotedFor(null);
        }
    }
}
