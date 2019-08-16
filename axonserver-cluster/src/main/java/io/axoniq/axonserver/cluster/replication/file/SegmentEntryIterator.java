package io.axoniq.axonserver.cluster.replication.file;

import io.axoniq.axonserver.grpc.cluster.Entry;
import org.springframework.data.util.CloseableIterator;

import java.nio.ByteBuffer;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Iterator that iterates through log entries in a single log entry file.
 *
 * @author Marc Gathier
 * @since 4.1
 */
public class SegmentEntryIterator implements CloseableIterator<Entry> {

    private final ByteBufferEntrySource source;
    private final AtomicLong nextIndex;

    /**
     * Creates the iterator.
     *
     * @param source    the buffer mapped to the file, current position is the position of the first log entry to read
     * @param nextIndex index of the first log entry to read
     */
    public SegmentEntryIterator(ByteBufferEntrySource source, long nextIndex) {
        this.source = source;
        this.nextIndex = new AtomicLong(nextIndex);
    }


    @Override
    public void close() {
        source.close();
    }

    @Override
    public boolean hasNext() {
        ByteBuffer buffer = source.getBuffer();
        int next = buffer.getInt(buffer.position());
        return next > 0;
    }

    @Override
    public Entry next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return source.readEvent(nextIndex.getAndIncrement());
    }

    /**
     * @return the current position in the file buffer
     */
    public int position() {
        return source.position();
    }
}
