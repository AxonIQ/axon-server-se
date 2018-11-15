package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.election.ElectionStore;
import io.axoniq.axonserver.grpc.cluster.RequestVoteRequest;

/**
 * @author Sara Pellegrini
 * @since 4.0
 */
public abstract class AbstractMembershipState implements MembershipState {

    private final RaftGroup raftGroup;

    AbstractMembershipState(Builder builder){
        builder.validate();
        this.raftGroup = builder.raftGroup;
    }

    public static Builder builder(){
        return new Builder();
    }

    public static class Builder {

        protected RaftGroup raftGroup;

        public Builder raftGroup(RaftGroup raftGroup){
            this.raftGroup = raftGroup;
            return this;
        }

        protected void validate(){
            if (raftGroup == null){
                throw new IllegalStateException("The RAFT group must be provided");
            }
        }

    }

    boolean voteGrantedFor(RequestVoteRequest request){
        if (votedFor() != null && !votedFor().equals(request.getCandidateId())) {
            return false;
        }

        if (request.getTerm() < currentTerm()) {
            return false;
        }

        if (request.getLastLogTerm() < lastLogTerm()) {
            return false;
        }

        if (request.getLastLogIndex() < lastLogIndex()) {
            return false;
        }

        markVotedFor(request.getCandidateId());
        return true;
    }

    String votedFor() {
        return raftGroup.localElectionStore().votedFor();
    }

    void markVotedFor(String candidateId) {
        raftGroup.localElectionStore().markVotedFor(candidateId);
    }

    long lastLogAppliedIndex() {
        return raftGroup.localLogEntryStore().lastAppliedIndex();
    }

    long lastLogTerm() {
        return raftGroup.localLogEntryStore().lastLogTerm();
    }

    long lastLogIndex() {
        return raftGroup.localLogEntryStore().lastLogIndex();
    }

    long currentTerm() {
        return raftGroup.localElectionStore().currentTerm();
    }

    String me() {
        return raftGroup.localNode().nodeId();
    }

    void updateCurrentTerm(long term){
        if (term > currentTerm()){
            ElectionStore electionStore = raftGroup.localElectionStore();
            electionStore.updateCurrentTerm(term);
            electionStore.markVotedFor(null);
        }
    }

    long maxElectionTimeout(){
        return raftGroup.maxElectionTimeout();
    }

    long minElectionTimeout(){
        return raftGroup.minElectionTimeout();
    }
}
