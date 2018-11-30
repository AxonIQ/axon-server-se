package io.axoniq.axonserver.cluster.replication.file;

import io.axoniq.axonserver.grpc.cluster.Entry;

import java.util.Iterator;

/**
 * Author: marc
 */
public class SegmentEntryIterator implements AutoCloseable, Iterator<Entry> {

    private final ByteBufferEntrySource duplicate;
    private volatile int lastPosition;
    private volatile long nextIndex;
    private volatile Entry nextEntry;

    public SegmentEntryIterator(ByteBufferEntrySource duplicate, long nextIndex) {
        this.duplicate = duplicate;
        this.nextIndex = nextIndex;
    }


    @Override
    public void close() {
        duplicate.close();
    }

    @Override
    public boolean hasNext() {
        lastPosition = duplicate.position();
        nextEntry = duplicate.readEvent(nextIndex);
        return nextEntry != null;
    }

    @Override
    public Entry next() {
        nextIndex++;
        return nextEntry;
    }

    public int startPosition() {
        return lastPosition;
    }
}
