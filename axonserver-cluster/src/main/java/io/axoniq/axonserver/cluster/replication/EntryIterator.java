package io.axoniq.axonserver.cluster.replication;

import io.axoniq.axonserver.cluster.TermIndex;
import io.axoniq.axonserver.grpc.cluster.Entry;

import java.util.Iterator;

/**
 * @author Marc Gathier
 * @since 4.1
 */
public interface EntryIterator extends Iterator<Entry>, AutoCloseable {

    @Override
    boolean hasNext();

    @Override
    Entry next();

    TermIndex previous();

    @Override
    default void close() {
    }
}
