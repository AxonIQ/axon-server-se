package io.axoniq.axonserver.cluster.replication;

import io.axoniq.axonserver.cluster.ReplicatorPeerStatus;
import io.axoniq.axonserver.cluster.rules.PrimaryMajorityAndActiveBackupsRule;

import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Checks if a commit index as returned by a peer causes transactions to be completed, based on the status of the other members.
 * Transaction is completed when majority of the primary nodes have committed the index and if there are
 * active backup nodes, at least n active backup nodes have committed the index.
 *
 * @author Marc Gathier
 * @since 4.1
 */
public class MajorityMatchStrategy implements MatchStrategy {

    private final Predicate<Long> primaryVotingAndActiveBackupsRule;

    /**
     * Constructor for the strategy
     *
     * @param lastLogIndexSupplier provides the last log index on the current node
     * @param replicatorPeers      other peers in the group
     */
    public MajorityMatchStrategy(Supplier<Long> lastLogIndexSupplier, Iterable<ReplicatorPeerStatus> replicatorPeers,
                                 Supplier<Integer> minActiveBackupsProvider) {
        this.primaryVotingAndActiveBackupsRule = new PrimaryMajorityAndActiveBackupsRule<>(replicatorPeers,
                                                                                           ReplicatorPeerStatus::role,
                                                                                           (peer, nextCommitCandidate) ->
                                                                                                   peer.matchIndex()
                                                                                                           >= nextCommitCandidate,
                                                                                           (nextCommitCandidate) ->
                                                                                                   nextCommitCandidate
                                                                                                           <= lastLogIndexSupplier
                                                                                                           .get(),
                                                                                           minActiveBackupsProvider
        );
    }

    @Override
    public boolean match(long nextCommitCandidate) {
        return primaryVotingAndActiveBackupsRule.test(nextCommitCandidate);
    }
}
