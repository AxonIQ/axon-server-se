package io.axoniq.axonserver.cluster.jpa;

import io.axoniq.axonserver.cluster.ProcessorStore;
import io.axoniq.axonserver.cluster.election.ElectionStore;

/**
 * @author Marc Gathier
 * @since 4.1
 */
public class JpaRaftStateController implements ElectionStore, ProcessorStore {
    private final String groupId;
    private final JpaRaftStateRepository repository;
    private JpaRaftState raftState;

    public JpaRaftStateController(String groupId, JpaRaftStateRepository repository) {
        this.groupId = groupId;
        this.repository = repository;
    }

    public void init() {
        repository.findById(groupId).ifPresent(state -> this.raftState = state);
        if( raftState == null) {
            this.raftState = repository.save(new JpaRaftState(groupId));
        }
    }

    @Override
    public void delete() {
        repository.deleteById(groupId);
    }

    @Override
    public String votedFor() {
        return raftState.getVotedFor();
    }

    @Override
    public void markVotedFor(String candidate) {
        raftState.setVotedFor(candidate);
        //sync();
    }

    @Override
    public long currentTerm() {
        return raftState.getCurrentTerm();
    }

    @Override
    public void updateCurrentTerm(long term) {
        raftState.setCurrentTerm(term);
        //sync();
    }

    @Override
    public void updateLastApplied(long lastAppliedIndex, long lastAppliedTerm) {
        raftState.setLastAppliedIndex(lastAppliedIndex);
        raftState.setLastAppliedTerm(lastAppliedTerm);
    }

    @Override
    public void updateCommit(long commitIndex, long commitTerm) {
        raftState.setCommitIndex(commitIndex);
        raftState.setCommitTerm(commitTerm);
    }

    @Override
    public long commitIndex() {
        return raftState.getCommitIndex();
    }

    @Override
    public long lastAppliedIndex() {
        return raftState.getLastAppliedIndex();
    }

    @Override
    public long commitTerm() {
        return raftState.commitTerm();
    }

    @Override
    public long lastAppliedTerm() {
        return raftState.lastAppliedTerm();
    }

    public void sync() {
        repository.save(raftState);
    }
}
