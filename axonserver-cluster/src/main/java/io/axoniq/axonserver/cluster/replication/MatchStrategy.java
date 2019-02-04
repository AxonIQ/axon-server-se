package io.axoniq.axonserver.cluster.replication;

import io.axoniq.axonserver.cluster.ReplicatorPeer;

import java.util.Collection;

/**
 * Match Strategy to determine if a commit candidate is matched. First implementation is {@link MajorityMatchStrategy},
 * which matches when an entry is stored in the majority of nodes. When a log entry is matched it can be applied on leader
 * and leader will send it as committed to the peers.
 * @author Marc Gathier
 */
public interface MatchStrategy {

    /**
     * Checks if nextCommitCandidate can be committed.
     * @param nextCommitCandidate the index of the log entry to check
     * @param replicatorPeers collection of replication peers (followers) containing their current state
     * @return true if candidate can be committed
     */
    boolean match(long nextCommitCandidate, Collection<? extends ReplicatorPeer> replicatorPeers);
}
