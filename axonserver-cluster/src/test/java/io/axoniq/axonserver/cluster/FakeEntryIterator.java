package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.replication.EntryIterator;
import io.axoniq.axonserver.grpc.cluster.Entry;

/**
 * @author Sara Pellegrini
 * @since
 */
public class FakeEntryIterator implements EntryIterator {

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public Entry next() {
        return null;
    }

    @Override
    public TermIndex previous() {
        return null;
    }
}
