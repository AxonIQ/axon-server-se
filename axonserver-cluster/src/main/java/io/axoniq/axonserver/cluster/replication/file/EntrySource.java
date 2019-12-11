package io.axoniq.axonserver.cluster.replication.file;


import io.axoniq.axonserver.grpc.cluster.Entry;

/**
 * Access to a file based log entry store.
 *
 * @author Marc Gathier
 * @Since 4.1
 */
public interface EntrySource extends AutoCloseable {

    int START_POSITION = 5;
    int TERM_OFFSET = 5;

    /**
     * Reads a log entry from a specific position in the file.
     *
     * @param position the position of the entry in the file
     * @param index    the index of the log entry
     * @return the log entry
     */
    Entry readLogEntry(int position, long index);

    default void close()  {
        // no-action
    }

    /**
     * Creates an iterator for a single segment of the log entry store.
     *
     * @param startIndex    the index of the first entry in the iterator
     * @param startPosition position of startIndex in the file
     * @return the iterator
     */
    SegmentEntryIterator createLogEntryIterator(long startIndex, int startPosition);

    /**
     * Retrieves the term associated with given {@code index}. Returns -1 if index was not found in log entry store.
     *
     * @param index the index of the log entry
     * @return term for log index
     */
    long readTerm(Integer index);
}
