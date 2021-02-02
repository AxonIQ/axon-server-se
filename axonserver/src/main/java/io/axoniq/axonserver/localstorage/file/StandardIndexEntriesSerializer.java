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
 * Serializer/Deserializer of {@link IndexEntries} to/from index file.
 *
 * @author Marc Gathier
 * @since 4.4
 */
public class StandardIndexEntriesSerializer implements Serializer<IndexEntries> {

    private static final StandardIndexEntriesSerializer INSTANCE = new StandardIndexEntriesSerializer();

    /**
     * Get an instance of the serializer.
     * @return an instance of the serializer
     */
    public static StandardIndexEntriesSerializer get() {
        return INSTANCE;
    }

    private StandardIndexEntriesSerializer() {
    }

    /**
     * Writes a value to a data output.
     * @param out the data output to write to
     * @param value the value to serialize
     * @throws IOException when writing to data output fails
     */
    @Override
    public void serialize(@Nonnull DataOutput2 out, @Nonnull IndexEntries value) throws IOException {
        out.packInt(value.size());
        out.packLong(value.firstSequenceNumber());
        for (Integer positionInfo : value.positions()) {
            out.packInt(positionInfo);
        }
    }

    /**
     * Reads a value from a data input
     * @param input the data input to read from
     * @param available number of bytes available
     * @return the deserialized object
     *
     * @throws IOException
     */
    @Override
    public IndexEntries deserialize(@Nonnull DataInput2 input, int available) throws IOException {
        int count = input.unpackInt();
        long sequenceNumber = input.unpackLong();
        List<Integer> entries = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            entries.add(input.unpackInt());
        }
        return new StandardIndexEntries(sequenceNumber, entries);
    }
}
