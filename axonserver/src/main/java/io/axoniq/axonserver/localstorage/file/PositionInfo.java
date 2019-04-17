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
 * @author Marc Gathier
 */
public class PositionInfo implements Serializable, Comparable<PositionInfo> {

    private final int position;
    private final long aggregateSequenceNumber;

    public PositionInfo(int position, long aggregateSequenceNumber) {

        this.position = position;
        this.aggregateSequenceNumber = aggregateSequenceNumber;
    }

    public int getPosition() {
        return position;
    }

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
