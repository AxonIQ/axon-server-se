package io.axoniq.axonserver.cluster.replication.file;

import io.axoniq.axonserver.grpc.cluster.Entry;

import java.util.Iterator;

/**
 * Author: marc
 */
public class SegmentEntryIterator implements AutoCloseable, Iterator<Entry> {

    private final ByteBufferEntrySource duplicate;
    private final long firstToken;
    private final long startToken;
    private final boolean validating;
    private volatile int lastPosition;
    private volatile long nextIndex;
    private volatile Entry nextEntry;

    public SegmentEntryIterator(ByteBufferEntrySource duplicate, long firstToken, long startToken, boolean validating) {
        this.duplicate = duplicate;
        this.firstToken = firstToken;
        this.startToken = startToken;
        this.validating = validating;
        moveTo(startToken);
    }

    private void moveTo(long startToken) {
        if( startToken > firstToken) {
            for( int i = 0 ; i < startToken - firstToken; i++) {
                duplicate.readEvent(firstToken+i);
            }
        }
        nextIndex = startToken;
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
