package io.axoniq.axonserver.cluster.replication.file;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

/**
 * Author: marc
 */
public class WritableEntrySource extends ByteBufferEntrySource {
    private static final Logger logger = LoggerFactory.getLogger(WritableEntrySource.class);
    public WritableEntrySource(ByteBuffer buffer, LogEntryTransformer eventTransformer) {
        super(buffer, eventTransformer);
    }

    private MappedByteBuffer mappedByteBuffer() {
        return (MappedByteBuffer)getBuffer();
    }

    public int limit() {
        return getBuffer().limit();
    }

    public int capacity() {
        return getBuffer().capacity();
    }

    public void force() {
        try {
            mappedByteBuffer().force();
        } catch( Exception ex) {
            logger.debug("Force failed", ex);
        }
    }

    public int getInt(int position) {
        return getBuffer().getInt(position);
    }


    public void putInt(int position, int value) {
        getBuffer().putInt(position, value);
    }

    public void position(int startPosition) {
        getBuffer().position(startPosition);
    }
}
