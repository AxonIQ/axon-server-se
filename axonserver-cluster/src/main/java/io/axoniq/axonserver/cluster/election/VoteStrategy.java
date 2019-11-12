package io.axoniq.axonserver.cluster.election;

import java.util.concurrent.CompletableFuture;

/**
 * @author Sara Pellegrini
 * @since 4.0
 */
public interface VoteStrategy {

    void registerVoteReceived(String voter, boolean granted);

    CompletableFuture<Election.Result> isWon();

}

