package io.axoniq.axonserver.cluster.replication;

import io.axoniq.axonserver.cluster.ReplicatorPeerStatus;

import java.util.function.Supplier;

/**
 * Checks if a commit index as returned by a peer causes transactions to be completed, based on the status of the other members.
 * Transaction is completed when majority of the primary nodes have committed the index and if there are
 * active backup nodes, the overall majority of primary and active backup nodes have committed the index and at least one
 * of the active backup nodes has committed the index.
 *
 * @author Marc Gathier
 * @since 4.1
 */
public class MajorityMatchStrategy implements MatchStrategy {

    private final Supplier<Long> lastLogIndexSupplier;
    private final Iterable<ReplicatorPeerStatus> replicatorPeers;

    /**
     * Constructor for the strategy
     *
     * @param lastLogIndexSupplier provides the last log index on the current node
     * @param replicatorPeers      other peers in the group
     */
    public MajorityMatchStrategy(Supplier<Long> lastLogIndexSupplier, Iterable<ReplicatorPeerStatus> replicatorPeers) {
        this.lastLogIndexSupplier = lastLogIndexSupplier;
        this.replicatorPeers = replicatorPeers;
    }

    @Override
    public boolean match(long nextCommitCandidate) {
        CommitIndexCheckResult candidatesCheckResult = new CommitIndexCheckResult(nextCommitCandidate);
        return candidatesCheckResult.primaryMajority() && candidatesCheckResult.fullMajority();
    }


    private class CommitIndexCheckResult {

        int primaryNodes;
        int primaryCount;
        int fullNodes;
        int fullCount;

        public CommitIndexCheckResult(long nextCommitCandidate) {
            primaryNodes = 1;
            fullNodes = 1;
            primaryCount = lastLogIndexSupplier.get() >= nextCommitCandidate ? 1 : 0;
            fullCount = lastLogIndexSupplier.get() >= nextCommitCandidate ? 1 : 0;

            for (ReplicatorPeerStatus replicatorPeer : replicatorPeers) {
                if (replicatorPeer.primaryNode()) {
                    primaryNodes++;
                    if (replicatorPeer.matchIndex() >= nextCommitCandidate) {
                        primaryCount++;
                    }
                }
                if (replicatorPeer.votingNode()) {
                    fullNodes++;
                    if (replicatorPeer.matchIndex() >= nextCommitCandidate) {
                        fullCount++;
                    }
                }
            }
        }

        public boolean fullMajority() {
            return fullCount >= (int) Math.ceil((fullNodes + 0.1f) / 2f) &&
                    (primaryNodes == fullNodes || fullCount > primaryCount);
        }

        public boolean primaryMajority() {
            return primaryCount >= (int) Math.ceil((primaryNodes + 0.1f) / 2f);
        }
    }
}
