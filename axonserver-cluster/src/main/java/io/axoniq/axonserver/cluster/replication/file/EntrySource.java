package io.axoniq.axonserver.cluster.replication.file;


import io.axoniq.axonserver.grpc.cluster.Entry;

/**
 * Author: marc
 */
public interface EntrySource extends AutoCloseable {

    Entry readEvent(int position, long index);

    default void close()  {
        // no-action
    }

    SegmentEntryIterator createEventIterator(long segment, long startToken, boolean validating);
}
