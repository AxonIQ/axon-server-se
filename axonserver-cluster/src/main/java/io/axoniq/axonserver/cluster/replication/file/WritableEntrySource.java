package io.axoniq.axonserver.cluster.replication.file;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.Arrays;

/**
 * Author: marc
 */
public class WritableEntrySource extends ByteBufferEntrySource {
    private static final Logger logger = LoggerFactory.getLogger(WritableEntrySource.class);
    public WritableEntrySource(ByteBuffer buffer, LogEntryTransformer eventTransformer, boolean cleanerHackNeeded) {
        super(buffer, eventTransformer, cleanerHackNeeded);
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

    public void clearFromPosition() {
        byte[] bytes = new byte[10240];
        Arrays.fill(bytes, (byte) 0);
        ByteBuffer buffer = getBuffer();
        int position = buffer.position();
        while( buffer.remaining() > 0) {
            buffer.put(bytes, 0, Math.min(bytes.length, buffer.remaining()));
        }
        buffer.position(position);
    }
}
