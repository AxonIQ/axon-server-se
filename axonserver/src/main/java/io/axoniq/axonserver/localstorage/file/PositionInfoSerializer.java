/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.file;

import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;

import java.io.IOException;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * @author Marc Gathier
 */
public class PositionInfoSerializer implements Serializer<SortedSet<PositionInfo>> {
    private static final PositionInfoSerializer positionInfoSerializer = new PositionInfoSerializer();

    public static PositionInfoSerializer get() {
        return positionInfoSerializer;
    }


    @Override
    public void serialize(DataOutput2 out, SortedSet<PositionInfo> value) throws IOException {
        out.packInt(value.size());
        out.packLong(value.first().getAggregateSequenceNumber());
        for(PositionInfo positionInfo: value) {
            out.packInt(positionInfo.getPosition());
        }
    }

    @Override
    public SortedSet<PositionInfo> deserialize(DataInput2 input, int available) throws IOException {
        int count = input.unpackInt();
        long sequenceNumber = input.unpackLong();
        SortedSet<PositionInfo> values = new ConcurrentSkipListSet<>();
        for( int i =0; i < count; i++) {
           values.add( new PositionInfo(input.unpackInt(), sequenceNumber++));
        }
        return values;
    }
}
