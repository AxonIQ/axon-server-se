package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axonserver.localstorage.SerializedEvent;
import io.axoniq.axonserver.localstorage.transformation.EventTransformer;
import io.axoniq.axonserver.localstorage.transformation.EventTransformerFactory;

import java.nio.ByteBuffer;

/**
 * @author Marc Gathier
 */
public class ByteBufferEventSource implements EventSource {


    private final EventTransformer eventTransformer;
    private final ByteBuffer buffer;
    private final boolean main;

    public ByteBufferEventSource(ByteBuffer buffer, EventTransformerFactory eventTransformerFactory, StorageProperties storageProperties) {
        byte version = buffer.get();
        int flags = buffer.getInt();
        this.eventTransformer = eventTransformerFactory.get(version, flags, storageProperties);
        this.buffer = buffer;
        this.main = true;
    }

    public ByteBufferEventSource(ByteBuffer buffer, EventTransformer eventTransformer) {
        this.buffer = buffer;
        this.eventTransformer = eventTransformer;
        this.main = false;
    }

    public SerializedEvent readEvent() {
        int size = buffer.getInt();
        byte[] bytes = new byte[size];
        buffer.get(bytes);
        return new SerializedEvent(bytes, eventTransformer);
    }

    public ByteBufferEventSource duplicate() {
        return new ByteBufferEventSource(buffer.duplicate(), eventTransformer);
    }

    @Override
    protected void finalize() {
        CleanUtils.cleanDirectBuffer(buffer, main, 60);
    }

    public SerializedEvent readEvent(int position) {
        buffer.position(position);
        return readEvent();
    }

    @Override
    public TransactionIterator createTransactionIterator(long segment, long token, boolean validating) {
        return new TransactionByteBufferIterator(this, segment, token, validating);
    }

    @Override
    public EventIterator createEventIterator(long segment, long startToken) {
        return new EventByteBufferIterator(this, segment, startToken);
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public int position() {
        return buffer.position();
    }

    public void clean(long delay) {
        CleanUtils.cleanDirectBuffer(getBuffer(), true, delay);
    }
}
