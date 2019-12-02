package io.axoniq.axonserver.cluster.election;

import io.axoniq.axonserver.cluster.MinMajority;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * @author Sara Pellegrini
 * @since 4.0
 */
public class MajorityStrategy implements VoteStrategy {

    private final Supplier<Integer> minMajority;
    private final Map<String, Boolean> votes = new ConcurrentHashMap<>();
    private final Logger log = LoggerFactory.getLogger(MajorityStrategy.class);
    private final CompletableFuture<Election.Result> won;

    public MajorityStrategy(Supplier<Integer> votersSize) {
        this.minMajority = new MinMajority(votersSize);
        won = new CompletableFuture<>();
    }

    @Override
    public void registerVoteReceived(String voter, boolean granted) {
        votes.put(voter, granted);
        long votesGranted = votes.values().stream().filter(voteGranted -> voteGranted).count();
        long votesRejected = votes.values().stream().filter(voteGranted -> !voteGranted).count();

        if (votesGranted >= minMajority.get()){
            log.info("Election is won with following votes: {}. MinMajority: {}.", votes, minMajority.get());
            won.complete(electionResult(true));
        } else if (votesRejected >= minMajority.get()) {
            log.info("Election is lost with following votes: {}. MinMajority: {}.", votes, minMajority.get());
            won.complete(electionResult(false));
        }

    }

    private Election.Result electionResult(boolean won) {
        return new Election.Result() {
            @Override
            public boolean won() {
                return won;
            }

            @Override
            public String cause() {
                return null;
            }
        };
    }

    @Override
    public CompletableFuture<Election.Result> isWon() {
        return won;
    }

    @Override
    public String toString() {
        return "MajorityStrategy {" +
                "minMajority=" + minMajority.get() +
                ", votes=" + votes +
                '}';
    }
}
