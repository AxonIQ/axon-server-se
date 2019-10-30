package io.axoniq.axonserver.cluster.election;

import io.axoniq.axonserver.cluster.RaftPeer;
import io.axoniq.axonserver.cluster.rules.PrimaryMajorityAndActiveBackupsRule;
import io.axoniq.axonserver.cluster.rules.PrimaryMajorityRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * An implementation of a voting strategy, that determines the overall result based on the majority in the primary nodes
 * and n active backup nodes.
 *
 * @author Marc Gathier
 * @since 4.3
 */
public class PrimaryAndVotingMajorityStrategy implements VoteStrategy {

    private final Logger log = LoggerFactory.getLogger(PrimaryAndVotingMajorityStrategy.class);
    private final Map<String, Boolean> votes = new ConcurrentHashMap<>();
    private final CompletableFuture<Election.Result> won = new CompletableFuture<>();
    private final Predicate<Boolean> majorityRejectedRule;
    private final Predicate<Boolean> majorityApprovedRule;


    public PrimaryAndVotingMajorityStrategy(Iterable<RaftPeer> voters, Supplier<Integer> minActiveBackupsProvider) {
        this.majorityRejectedRule = new PrimaryMajorityRule<>(voters,
                                                              RaftPeer::role,
                                                              (node, expectedResult) -> vote(node.nodeId(),
                                                                                             expectedResult),
                                                              expectedResult -> expectedResult);
        this.majorityApprovedRule = new PrimaryMajorityAndActiveBackupsRule<>(voters,
                                                                              RaftPeer::role,
                                                                              (node, expectedResult) -> vote(node.nodeId(),
                                                                                                             expectedResult),
                                                                              expectedResult -> expectedResult,
                                                                              minActiveBackupsProvider);
    }

    private boolean vote(String nodeId, boolean expected) {
        Boolean actualVote = votes.get(nodeId);
        return actualVote != null && actualVote == expected;
    }


    @Override
    public void registerVoteReceived(String voter, boolean granted, boolean goAway) {
        if (goAway) {
            log.info("Received goAway from {}", voter);
            won.complete(electionResult(false, true));
            return;
        }
        votes.put(voter, granted);

        if (majorityRejectedRule.test(false)) {
            won.complete(electionResult(false, false));
        } else {
            if (majorityApprovedRule.test(true)) {
                won.complete(electionResult(true, false));
            }
        }

    }

    private Election.Result electionResult(boolean won, boolean goAway) {
        return new Election.Result() {
            @Override
            public boolean won() {
                return won;
            }

            @Override
            public boolean goAway() {
                return goAway;
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
        return "PrimaryAndVotingMajorityStrategy {" +
                "votes=" + votes +
                '}';
    }
}
