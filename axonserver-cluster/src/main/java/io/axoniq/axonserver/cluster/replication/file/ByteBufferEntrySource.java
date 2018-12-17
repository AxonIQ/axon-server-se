package io.axoniq.axonserver.cluster.replication.file;

import io.axoniq.axonserver.cluster.exception.ErrorCode;
import io.axoniq.axonserver.cluster.exception.LogException;
import io.axoniq.axonserver.grpc.cluster.Config;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.cluster.SerializedObject;

import java.nio.ByteBuffer;

/**
 * Author: marc
 */
public class ByteBufferEntrySource implements EntrySource {


    private final LogEntryTransformer eventTransformer;
    private final ByteBuffer buffer;
    private final boolean main;

    public ByteBufferEntrySource(ByteBuffer buffer, LogEntryTransformerFactory eventTransformerFactory, StorageProperties storageProperties) {
        byte version = buffer.get();
        int flags = buffer.getInt();
        this.eventTransformer = eventTransformerFactory.get(version, flags, storageProperties);
        this.buffer = buffer;
        this.main = true;
    }

    public ByteBufferEntrySource(ByteBuffer buffer, LogEntryTransformer eventTransformer) {
        this.buffer = buffer;
        this.eventTransformer = eventTransformer;
        this.main = false;
    }

    public ByteBufferEntrySource(ByteBuffer duplicate, LogEntryTransformer eventTransformer, int startPosition) {
        this(duplicate, eventTransformer);
        buffer.position(startPosition);
    }

    public Entry readEvent(long index) {
        try {
            int size = buffer.getInt();
            if( size ==0 || size ==-1) {
                System.out.println(index + ": Position: " + buffer.position());
                return null;
            }

            buffer.get(); // version
            long term = buffer.getLong();
            int type = buffer.getInt();
            Entry.DataCase dataCase = Entry.DataCase.forNumber(type);
            byte[] bytes = new byte[size];
            buffer.get(bytes);
            bytes = eventTransformer.readLogEntry(bytes);
            Entry.Builder builder = Entry.newBuilder().setTerm(term).setIndex(index);
            switch (dataCase) {
                case SERIALIZEDOBJECT:
                        builder.setSerializedObject(SerializedObject.parseFrom(bytes));
                    break;
                case NEWCONFIGURATION:
                    builder.setNewConfiguration(Config.parseFrom(bytes));
                    break;
                case DATA_NOT_SET:
                    break;
            }
            buffer.getInt(); //CRC
            return builder.build();
        } catch (Exception e) {
            throw new LogException(ErrorCode.DATAFILE_READ_ERROR, "failed to read entry: " + index, e);
        }
    }

    public ByteBufferEntrySource duplicate() {
        return new ByteBufferEntrySource(buffer.duplicate(), eventTransformer);
    }
    public ByteBufferEntrySource duplicate(int startPosition) {
        return new ByteBufferEntrySource(buffer.duplicate(), eventTransformer, startPosition);
    }

    @Override
    public SegmentEntryIterator createLogEntryIterator(long segment, long startIndex, int startPosition, boolean validating) {
        return new SegmentEntryIterator(duplicate(startPosition), startIndex);
    }

    @Override
    protected void finalize() {
        CleanUtils.cleanDirectBuffer(buffer, main, 60);
    }

    public Entry readLogEntry(int position, long index) {
        buffer.position(position);
        return readEvent(index);
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
