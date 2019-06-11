/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.file;

import java.io.Serializable;
import java.util.Objects;

/**
 * Info about the position of the event in the segment file per aggregate.
 *
 * @author Marc Gathier
 */
public class PositionInfo implements Serializable, Comparable<PositionInfo> {

    private final int position;
    private final long aggregateSequenceNumber;

    /**
     * Creates the info about the position of the event in the segment file per aggregate.
     *
     * @param position                byte position of the event in the segment file
     * @param aggregateSequenceNumber overall aggregate sequence number of the event
     */
    public PositionInfo(int position, long aggregateSequenceNumber) {

        this.position = position;
        this.aggregateSequenceNumber = aggregateSequenceNumber;
    }

    /**
     * Gets byte position of the event in the segment file.
     *
     * @return byte position of the event in the segment file
     */
    public int getPosition() {
        return position;
    }

    /**
     * Gets the overall aggregate sequence number of the event.
     *
     * @return the overall aggregate sequence number of the event
     */
    public long getAggregateSequenceNumber() {
        return aggregateSequenceNumber;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PositionInfo that = (PositionInfo) o;
        return aggregateSequenceNumber == that.aggregateSequenceNumber;
    }

    @Override
    public int hashCode() {
        return Objects.hash(aggregateSequenceNumber);
    }

    @Override
    public int compareTo(PositionInfo o) {
        return Long.compare(this.aggregateSequenceNumber, o.aggregateSequenceNumber);
    }
}
