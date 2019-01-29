package io.axoniq.axonserver.cluster.election;

import io.axoniq.axonserver.cluster.MinMajority;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private final Logger log = LoggerFactory.getLogger(MajorityElection.class);

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
        boolean won = voteGranted >= minMajority.get();
        if (won && log.isInfoEnabled()){
            log.info("Election is won with following votes: {}", votes);
        }
        return won;
    }
}
