package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axonserver.localstorage.SerializedEvent;
import io.axoniq.axonserver.localstorage.transformation.EventTransformer;
import io.axoniq.axonserver.localstorage.transformation.EventTransformerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Marc Gathier
 */
public class ByteBufferEventSource implements EventSource {


    private final EventTransformer eventTransformer;
    private final Runnable onClose;
    private final ByteBuffer buffer;
    private final boolean main;
    private final AtomicInteger duplicatesCount = new AtomicInteger();
    private final String path;

    public ByteBufferEventSource(String path, ByteBuffer buffer, EventTransformerFactory eventTransformerFactory, StorageProperties storageProperties) {
        this.path = path;
        byte version = buffer.get();
        int flags = buffer.getInt();
        this.eventTransformer = eventTransformerFactory.get(version, flags, storageProperties);
        this.buffer = buffer;
        this.main = true;
        this.onClose = null;
    }

    protected ByteBufferEventSource(String path, ByteBuffer buffer, EventTransformer eventTransformer, Runnable onClose) {
        this.path = path;
        this.buffer = buffer;
        this.eventTransformer = eventTransformer;
        this.onClose = onClose;
        this.main = false;
    }

    protected ByteBufferEventSource(String path, ByteBuffer buffer, EventTransformer eventTransformer) {
        this.path = path;
        this.buffer = buffer;
        this.eventTransformer = eventTransformer;
        this.onClose = null;
        this.main = true;
    }

    public SerializedEvent readEvent() {
        int size = buffer.getInt();
        byte[] bytes = new byte[size];
        buffer.get(bytes);
        return new SerializedEvent(eventTransformer.fromStorage(bytes));
    }

    public ByteBufferEventSource duplicate() {
        duplicatesCount.incrementAndGet();
        return new ByteBufferEventSource(path, buffer.duplicate(), eventTransformer, duplicatesCount::decrementAndGet);
    }

    @Override
    protected void finalize() {
        if( main) {
            CleanUtils.cleanDirectBuffer(buffer, () -> duplicatesCount.get() == 0, 60, path);
        }
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
        if( main ) {
            CleanUtils.cleanDirectBuffer(getBuffer(), () -> duplicatesCount.get() == 0, delay, path);
        }
    }

    @Override
    public void close() {
        if( onClose != null) {
            onClose.run();
        }
    }
}
