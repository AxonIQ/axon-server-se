/*
 * Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.file;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

/**
 * Calculates the CRC32 checksum of a given {@link ByteBuffer}, without loading the full content in memory.
 *
 * @author Marc Gathier
 * @author Sara Pellegrini
 * @author Milan Savic
 */
public class Checksum {

    private static final int FOUR_MB = 1024 * 1024 * 4;
    private final CRC32 crc32;
    private final int batch;

    /**
     * Constructs an instance that updates the checksum value loading data in chucks of 4 MBs.
     */
    public Checksum() {
        this(FOUR_MB);
    }

    public Checksum(int batch) {
        crc32 = new CRC32();
        crc32.reset();
        this.batch = batch;
    }

    public int get() {
        return (int) crc32.getValue();
    }

    public Checksum update(byte[] bytes) {
        crc32.update(bytes);
        return this;
    }

    /**
     * Update the checksum with the specified data.
     *
     * @param buffer   the buffer that provides the data to be used to calculate the checksum
     * @param position the initial position in the buffer of the data to be used to calculate the checksum
     * @param size     the size of the data to be used to calculate checksum
     * @return this instance, for fluent api
     */
    public Checksum update(ByteBuffer buffer, int position, int size) {
        if (position < 0) {
            throw new IllegalArgumentException("The position cannot be lower than 0");
        }
        if (size > buffer.limit() - position) {
            throw new IllegalArgumentException("The ByteBuffer is smaller than expected");
        }
        byte[] bytes = new byte[batch];
        ByteBuffer b = (ByteBuffer) buffer.duplicate().position(position);

        int rem = size;
        while (rem > 0) {
            int batchSize = Math.min(batch, rem);
            b.get(bytes, 0, batchSize);
            crc32.update(bytes, 0, batchSize);
            rem -= batchSize;
        }
        return this;
    }

}
