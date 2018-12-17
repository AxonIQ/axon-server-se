package io.axoniq.axonserver.cluster.replication.file;


import io.axoniq.axonserver.grpc.cluster.Entry;

/**
 * Author: marc
 */
public interface EntrySource extends AutoCloseable {

    Entry readLogEntry(int position, long index);

    default void close()  {
        // no-action
    }

    SegmentEntryIterator createLogEntryIterator(long segment, long startIndex, int startPosition, boolean validating);
}
