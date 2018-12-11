package io.axoniq.axonserver.cluster.election;

/**
 * @author Sara Pellegrini
 * @since 4.0
 */
public interface Election {

    void registerVoteReceived(String voter, boolean granted);

    boolean isWon();

}

