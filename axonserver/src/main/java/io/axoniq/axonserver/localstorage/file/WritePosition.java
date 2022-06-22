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
    public int entries;

    public WritePosition(long sequence, int position) {
        this(sequence, position, null, null, 0);
    }

    public WritePosition(long sequence, int position, WritableEventSource buffer, Long segment, int entries) {
        this.sequence = sequence;
        this.position = position;
        this.buffer = buffer;
        this.segment = segment;
    }

    public WritePosition reset(WritableEventSource buffer) {
        return new WritePosition(sequence,
                                 SegmentBasedEventStore.VERSION_BYTES + SegmentBasedEventStore.FILE_OPTIONS_BYTES,
                                 buffer,
                                 sequence,
                                 entries);
    }

    boolean isOverflow(int transactionLength) {
        return buffer != null
                && position + 4 < buffer.limit()
                && position + transactionLength + 4 >= buffer.limit();
    }

    Long getSegment() {
        return segment;
    }

    int getPosition() {
        return position;
    }

    boolean isWritable(int transactionLength) {
        return this != INVALID && buffer != null && position + transactionLength + 4 < buffer.capacity();
    }

    WritePosition incrementedWith(long sequence, int position, int nrOfEvents) {
        if (this == INVALID || this.position > buffer.capacity()) {
            return INVALID;
        }
        return new WritePosition(
                this.sequence + sequence,
                this.position + position,
                this.buffer, this.segment, nrOfEvents);
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
