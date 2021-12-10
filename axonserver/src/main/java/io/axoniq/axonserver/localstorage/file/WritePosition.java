/*
 *  Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
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

    static final WritePosition INVALID = new WritePosition(Long.MAX_VALUE, Integer.MAX_VALUE, -1, null, null);
    private static final Comparator<WritePosition> writePositionComparator =
            Comparator.comparingLong(WritePosition::getSegment)
                      .thenComparingInt(WritePosition::getPosition);
    final long sequence;
    final int position;
    final int version;
    final WritableEventSource buffer;
    final Long segment;

    public WritePosition(long sequence, int position, int version) {
        this(sequence, position, version, null, null);
    }

    public WritePosition(long sequence, int position, int version, WritableEventSource buffer, Long segment) {
        this.sequence = sequence;
        this.position = position;
        this.version = version;
        this.buffer = buffer;
        this.segment = segment;
    }

    public WritePosition reset(WritableEventSource buffer, int version) {
        return new WritePosition(sequence,
                                 SegmentBasedEventStore.VERSION_BYTES + SegmentBasedEventStore.FILE_OPTIONS_BYTES,
                                 version,
                                 buffer,
                                 sequence);
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

    WritePosition incrementedWith(long sequence, int position, int version) {
        if (this == INVALID || this.position > buffer.capacity() || this.version == -1) {
            return INVALID;
        }
        return new WritePosition(
                this.sequence + sequence,
                this.position + position,
                this.version == version ? version : -1,
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

    public boolean isVersionUpdate(int version) {
        return this.version >= 0 && this.version != version;
    }
}
