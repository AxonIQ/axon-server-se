package io.axoniq.axonserver.cluster.election;

/**
 * @author Sara Pellegrini
 * @since 4.0
 */
public interface VoteStrategy {

    void registerVoteReceived(String voter, boolean granted);

    boolean isWon();

}

