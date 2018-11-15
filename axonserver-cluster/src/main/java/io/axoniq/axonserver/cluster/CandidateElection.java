package io.axoniq.axonserver.cluster;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Sara Pellegrini
 * @since 4.0
 */
public class CandidateElection implements Election{

    private final long votersSize;
    private final Map<String, Boolean> votes = new ConcurrentHashMap<>();

    public CandidateElection(long votersSize) {
        this.votersSize = votersSize;
    }

    @Override
    public void onVoteReceived(String voter, boolean granted) {
        votes.put(voter, granted);
    }

    @Override
    public boolean isWon() {
        long voteGranted = votes.values().stream().filter(granted -> granted).count();
        return voteGranted >= minMajority();
    }

    private long minMajority() {
        return (votersSize / 2) + (votersSize % 2 == 0 ? 0L : 1L);
    }
}
