package io.axoniq.axonserver.cluster.election;

/**
 * @author Sara Pellegrini
 * @since 4.0
 */
public interface ElectionStore {

    String votedFor();

    void markVotedFor(String candidate);

    long currentTerm();

    void updateCurrentTerm(long term);

}
