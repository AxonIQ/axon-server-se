package io.axoniq.axonserver.filestorage.impl;

import io.axoniq.axonserver.filestorage.FileStoreEntry;
import org.springframework.data.util.CloseableIterator;

import java.nio.ByteBuffer;

/**
 * @author Marc Gathier
 * @since 4.6.0
 */
public class ByteBufferEntrySource {
    private final int START_POSITION = 5;

    private final ByteBuffer buffer;
    private final long segment;
    private final boolean main;
    private final boolean cleanerHackNeeded;

    public ByteBufferEntrySource(ByteBuffer buffer, StorageProperties storageProperties, long segment) {
        this.buffer = buffer;
        this.segment = segment;
        this.buffer.position(START_POSITION);
        this.main = true;
        this.cleanerHackNeeded = storageProperties.isForceClean();
    }

    public ByteBufferEntrySource(ByteBuffer buffer, boolean cleanerHackNeeded, long segment) {
        this.buffer = buffer;
        this.main = false;
        this.segment = segment;
        this.cleanerHackNeeded = cleanerHackNeeded;
    }

    public ByteBufferEntrySource(ByteBuffer duplicate, int startPosition, long segment) {
        this(duplicate, false, segment);
        buffer.position(startPosition);
    }

    public FileStoreEntry readEntry() {
        try {
            int size = buffer.getInt();
            if( size ==0 || size ==-1) {
                return null;
            }

            byte version = buffer.get(); // version
            byte[] bytes = new byte[size];
            buffer.get(bytes);
            buffer.getInt(); //CRC
            return new FileStoreEntry() {
                @Override
                public byte[] bytes() {
                    return bytes;
                }

                @Override
                public byte version() {
                    return version;
                }
            };
        } catch (Exception e) {
            throw new FileStoreException(FileStoreErrorCode.DATAFILE_READ_ERROR, "failed to read entry", e);
        }
    }

    public CloseableIterator<FileStoreEntry> createEntryIterator(long startIndex) {
        return new BufferEntryIterator(duplicate(START_POSITION), segment, startIndex);
    }

    public ByteBufferEntrySource duplicate() {
        return new ByteBufferEntrySource(buffer.duplicate(), cleanerHackNeeded, segment);
    }
    public ByteBufferEntrySource duplicate(int startPosition) {
        return new ByteBufferEntrySource(buffer.duplicate(), startPosition, segment);
    }

    @Override
    protected void finalize() {
        if (cleanerHackNeeded) CleanUtils.cleanDirectBuffer(buffer, () -> main, 60, "filename");
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public int position() {
        return buffer.position();
    }

    public void clean(long delay) {
        if (cleanerHackNeeded) CleanUtils.cleanDirectBuffer(getBuffer(), () -> true, delay, "filename");
    }
}
