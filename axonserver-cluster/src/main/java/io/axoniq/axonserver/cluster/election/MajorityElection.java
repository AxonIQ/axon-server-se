package io.axoniq.axonserver.cluster.election;

import io.axoniq.axonserver.cluster.MinMajority;
import io.axoniq.axonserver.cluster.election.Election;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * @author Sara Pellegrini
 * @since 4.0
 */
public class MajorityElection implements Election {

    private final Supplier<Integer> minMajority;
    private final Map<String, Boolean> votes = new ConcurrentHashMap<>();

    public MajorityElection(Supplier<Integer> votersSize) {
        this.minMajority = new MinMajority(votersSize);
    }

    @Override
    public void registerVoteReceived(String voter, boolean granted) {
        votes.put(voter, granted);
    }

    @Override
    public boolean isWon() {
        long voteGranted = votes.values().stream().filter(granted -> granted).count();
        return voteGranted >= minMajority.get();
    }
}
