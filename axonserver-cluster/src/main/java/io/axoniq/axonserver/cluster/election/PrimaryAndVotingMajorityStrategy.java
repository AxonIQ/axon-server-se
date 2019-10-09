package io.axoniq.axonserver.cluster.election;

import io.axoniq.axonserver.cluster.RaftPeer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An implementation of a voting strategy, that determines the overall result based on the majority in the primary nodes
 * and the majority of all voting members.
 *
 * @author Marc Gathier
 * @since 4.2
 */
public class PrimaryAndVotingMajorityStrategy implements VoteStrategy {

    private static final int PRIMARY_GRANTED_IDX = 0;
    private static final int PRIMARY_REJECTED_IDX = 1;
    private static final int GRANTED_IDX = 2;
    private static final int REJECTED_IDX = 3;

    private final Logger log = LoggerFactory.getLogger(PrimaryAndVotingMajorityStrategy.class);
    private final Map<String, Boolean> votes = new ConcurrentHashMap<>();
    private final Set<String> nonPrimaryNodes = new HashSet<>();
    private final int primaryMajority;
    private final int fullMajority;
    private final CompletableFuture<Election.Result> won = new CompletableFuture<>();

    public PrimaryAndVotingMajorityStrategy(Iterable<RaftPeer> voters) {
        int votingNodes = 1;
        int primaryNodes = 1;
        for (RaftPeer voter : voters) {
            if (voter.primaryNode()) {
                primaryNodes++;
            } else if (voter.votingNode()) {
                nonPrimaryNodes.add(voter.nodeId());
                votingNodes++;
            }
        }
        primaryMajority = (int) Math.ceil((primaryNodes + 0.1) / 2f);
        fullMajority = (int) Math.ceil((votingNodes + 0.1) / 2f);
    }

    @Override
    public void registerVoteReceived(String voter, boolean granted, boolean goAway) {
        if (goAway) {
            log.info("Received goAway from {}", voter);
            won.complete(electionResult(false, true));
            return;
        }
        votes.put(voter, granted);
        int[] counters = {0, 0, 0, 0};

        votes.forEach((node, approved) -> {
            if (approved) {
                counters[GRANTED_IDX]++;
                if (!nonPrimaryNodes.contains(node)) {
                    counters[PRIMARY_GRANTED_IDX]++;
                }
            } else {
                counters[REJECTED_IDX]++;
                if (!nonPrimaryNodes.contains(node)) {
                    counters[PRIMARY_REJECTED_IDX]++;
                }
            }
        });

        if (counters[PRIMARY_GRANTED_IDX] >= primaryMajority) {

            if (counters[GRANTED_IDX] >= fullMajority && (
                    nonPrimaryNodes.isEmpty() || counters[GRANTED_IDX] > counters[PRIMARY_GRANTED_IDX]
            )) {
                log.info("Election is won with following votes: {}. MinMajority: {}.", votes, fullMajority);
                won.complete(electionResult(true, false));
            } else if (counters[REJECTED_IDX] >= fullMajority) {
                log.info("Election is lost on all voting nodes with following votes: {}. MinMajority: {}.",
                         votes,
                         fullMajority);
                won.complete(electionResult(false, false));
            }
        } else if (counters[PRIMARY_REJECTED_IDX] >= primaryMajority) {
            log.info("Election is lost on primary nodes with following votes: {}. MinMajority: {}.",
                     votes,
                     primaryMajority);
            won.complete(electionResult(false, false));
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
                "primaryMajority=" + primaryMajority +
                "fullMajority=" + fullMajority +
                ", votes=" + votes +
                '}';
    }
}
