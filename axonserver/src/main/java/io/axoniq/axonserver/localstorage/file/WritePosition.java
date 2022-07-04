/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.file;

import java.util.Comparator;

/**
 * Describes a block where the event store can write a transaction.
 *
 * @author Marc Gathier
 */
public class WritePosition implements Comparable<WritePosition> {

    static final WritePosition INVALID = new WritePosition(Long.MAX_VALUE, Integer.MAX_VALUE, null, null, 0);
    private static final Comparator<WritePosition> writePositionComparator =
            Comparator.comparingLong(WritePosition::getSegment)
                      .thenComparingInt(WritePosition::getPosition);
    final long sequence;
    final int position;
    final WritableEventSource buffer;
    final Long segment;
    final int prevEntries;

    /**
     * @param sequence the sequence number of the first event in this block
     * @param position the position of the block in the file
     * @param buffer the buffer for writing the block
     * @param segment the segment number containing the block
     * @param prevEntries the number of events in the previous transaction
     */
    public WritePosition(long sequence, int position, WritableEventSource buffer, Long segment, int prevEntries) {
        this.sequence = sequence;
        this.position = position;
        this.buffer = buffer;
        this.segment = segment;
        this.prevEntries = prevEntries;
    }

    /**
     * Creates a new version of the {@link WritePosition} linking to a new write buffer. Position is the initial position
     * in the segment to start writing event data.
     * @param buffer the write buffer for the new segment
     * @return updated write position
     */
    public WritePosition reset(WritableEventSource buffer) {
        return new WritePosition(sequence,
                                 SegmentBasedEventStore.VERSION_BYTES + SegmentBasedEventStore.FILE_OPTIONS_BYTES,
                                 buffer,
                                 sequence,
                                 prevEntries);
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

    WritePosition incrementedWith(int entries, int position) {
        if (this == INVALID || this.position > buffer.capacity()) {
            return INVALID;
        }
        return new WritePosition(
                this.sequence + entries,
                this.position + position,
                this.buffer, this.segment, entries);
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
}
