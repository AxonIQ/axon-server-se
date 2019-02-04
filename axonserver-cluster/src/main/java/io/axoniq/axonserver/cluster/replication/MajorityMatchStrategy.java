package io.axoniq.axonserver.cluster.replication;

import io.axoniq.axonserver.cluster.ReplicatorPeer;

import java.util.Collection;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * @author Marc Gathier
 */
public class MajorityMatchStrategy implements MatchStrategy {

    private final Supplier<Long> lastLogIndexSupplier;

    public MajorityMatchStrategy(Supplier<Long> lastLogIndexSupplier) {
        this.lastLogIndexSupplier = lastLogIndexSupplier;
    }

    @Override
    public boolean match(long nextCommitCandidate, Collection<? extends ReplicatorPeer> replicatorPeers) {
        int majority = (int) Math.ceil((replicatorPeers.size() + 1.1) / 2f);
        Stream<Long> matchIndexes = Stream.concat(Stream.of(lastLogIndexSupplier.get()),
                                                  replicatorPeers.stream()
                                                                   .map(ReplicatorPeer::matchIndex));
        return matchIndexes.filter(p -> p >= nextCommitCandidate).count() >= majority;
    }
}
