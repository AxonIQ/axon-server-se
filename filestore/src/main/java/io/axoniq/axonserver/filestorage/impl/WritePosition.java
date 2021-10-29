package io.axoniq.axonserver.filestorage.impl;

import java.util.Comparator;

/**
 * @author Marc Gathier
 * @since 4.6.0
 */
public class WritePosition implements Comparable<WritePosition> {
    static final WritePosition INVALID = new WritePosition(Long.MAX_VALUE, Integer.MAX_VALUE, null, null);
    private static final Comparator<WritePosition> writePositionComparator =
            Comparator.comparingLong(WritePosition::getSegment)
                      .thenComparingInt(WritePosition::getPosition);
    final long sequence;
    final int position;
    final WritableEntrySource buffer;
    final Long segment;

    WritePosition(long sequence, int position) {
        this(sequence, position, null, null);
    }

    WritePosition(long sequence, int position, WritableEntrySource buffer, Long segment) {
        this.sequence = sequence;
        this.position = position;
        this.buffer = buffer;
        this.segment = segment;
    }

    WritePosition reset(WritableEntrySource buffer) {
        return new WritePosition(sequence, 5, buffer, sequence);
    }

    boolean isOverflow(int transactionLength) {
        return buffer != null
                && position + 4 <= buffer.capacity()
                && position + transactionLength + 4 > buffer.capacity();
    }

    Long getSegment() {
        return segment;
    }

    int getPosition() {
        return position;
    }

    boolean isWritable(int transactionLength) {
        return this != INVALID && buffer != null && position + transactionLength + 4 <= buffer.capacity();
    }

    WritePosition incrementedWith(long sequence, int position) {
        if (this == INVALID || this.position > buffer.capacity()) {
            return INVALID;
        }
        return new WritePosition(
                this.sequence + sequence,
                this.position + position,
                this.buffer, this.segment);
    }

    boolean isComplete() {
        return buffer.getInt(position) != 0;
    }

    @Override
    public String toString() {
        return "WritePosition{" +
                "sequence=" + sequence +
                ", position=" + position +
                ", segment=" + segment +
                '}';
    }

    @Override
    public int compareTo( WritePosition that) {
        return writePositionComparator.compare(this, that);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        WritePosition that = (WritePosition) o;

        return position == that.position && segment != null ? segment.equals(that.segment) : that.segment == null;
    }

    @Override
    public int hashCode() {
        int result = position;
        result = 31 * result + (segment != null ? segment.hashCode() : 0);
        return result;
    }

    public void force() {
        buffer.force();
    }

    private long index;

}
