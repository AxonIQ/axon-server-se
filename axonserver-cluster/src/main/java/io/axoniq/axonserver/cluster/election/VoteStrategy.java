package io.axoniq.axonserver.cluster.election;

import java.util.concurrent.CompletableFuture;

/**
 * @author Sara Pellegrini
 * @since 4.0
 */
public interface VoteStrategy {

    default void registerVoteReceived( String voter, boolean granted) {
        registerVoteReceived(voter, granted, false);
    }

    void registerVoteReceived(String voter, boolean granted, boolean goAway);

    CompletableFuture<Election.Result> isWon();

}

