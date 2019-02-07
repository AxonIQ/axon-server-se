package io.axoniq.axonserver.cluster.replication;

import com.google.common.collect.Iterables;
import io.axoniq.axonserver.cluster.ReplicatorPeer;

import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author Marc Gathier
 */
public class MajorityMatchStrategy implements MatchStrategy {

    private final Supplier<Long> lastLogIndexSupplier;
    private final Iterable<ReplicatorPeer> replicatorPeers;

    public MajorityMatchStrategy(Supplier<Long> lastLogIndexSupplier, Iterable<ReplicatorPeer> replicatorPeers) {
        this.lastLogIndexSupplier = lastLogIndexSupplier;
        this.replicatorPeers = replicatorPeers;
    }

    @Override
    public boolean match(long nextCommitCandidate) {

        int majority = (int) Math.ceil((size(replicatorPeers) + 1.1) / 2f);
        Stream<Long> matchIndexes = Stream.concat(Stream.of(lastLogIndexSupplier.get()),
                                                  StreamSupport.stream(replicatorPeers.spliterator(), false)
                                                                   .map(ReplicatorPeer::matchIndex));
        return matchIndexes.filter(p -> p >= nextCommitCandidate).count() >= majority;
    }

    private int size(Iterable<?> replicatorPeers) {
        return Iterables.size(replicatorPeers);
    }
}
