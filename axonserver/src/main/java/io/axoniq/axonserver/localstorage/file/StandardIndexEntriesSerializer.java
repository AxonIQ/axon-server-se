/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
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
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;

/**
 * @author Marc Gathier
 */
public class StandardIndexEntriesSerializer implements Serializer<IndexEntries> {

    private static final StandardIndexEntriesSerializer INSTANCE = new StandardIndexEntriesSerializer();

    public static StandardIndexEntriesSerializer get() {
        return INSTANCE;
    }

    private StandardIndexEntriesSerializer() {

    }

    @Override
    public void serialize(@Nonnull DataOutput2 out, @Nonnull IndexEntries value) throws IOException {
        out.packInt(value.size());
        out.packLong(value.firstSequenceNumber());
        for (IndexEntry positionInfo : value.positions()) {
            out.packInt(positionInfo.getPosition());
        }
    }

    @Override
    public IndexEntries deserialize(@Nonnull DataInput2 input, int available) throws IOException {
        int count = input.unpackInt();
        long sequenceNumber = input.unpackLong();
        List<IndexEntry> entryList = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            entryList.add(new IndexEntry(sequenceNumber + i, input.unpackInt()));
        }
        return new StandardIndexEntries(sequenceNumber, entryList);
    }
}
