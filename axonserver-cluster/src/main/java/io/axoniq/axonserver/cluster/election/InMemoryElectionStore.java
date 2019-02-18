package io.axoniq.axonserver.cluster.election;

/**
 * @author Milan Savic
 */
public class InMemoryElectionStore implements ElectionStore {

    private volatile String votedFor;
    private volatile long currentTerm;

    @Override
    public String votedFor() {
        return votedFor;
    }

    @Override
    public void markVotedFor(String candidate) {
        this.votedFor = candidate;
    }

    @Override
    public long currentTerm() {
        return currentTerm;
    }

    @Override
    public void updateCurrentTerm(long term) {
        this.currentTerm = term;
    }

    @Override
    public void delete() {
        votedFor = null;
        currentTerm = 0;
    }
}
