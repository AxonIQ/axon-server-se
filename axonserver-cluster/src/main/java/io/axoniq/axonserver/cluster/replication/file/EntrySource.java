package io.axoniq.axonserver.cluster.replication.file;


import io.axoniq.axonserver.grpc.cluster.Entry;

/**
 * @author Marc Gathier
 */
public interface EntrySource extends AutoCloseable {

    Entry readLogEntry(int position, long index);

    default void close()  {
        // no-action
    }

    SegmentEntryIterator createLogEntryIterator(long segment, long startIndex, int startPosition, boolean validating);
}
