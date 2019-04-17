/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
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
 * @author Marc Gathier
 */
public class Checksum {
    private final CRC32 crc32;

    public Checksum() {
        crc32 = new CRC32();
        crc32.reset();
    }

    public int get() {
        return (int) crc32.getValue();
    }

    public Checksum update(byte[] bytes) {
        crc32.update(bytes);
        return this;
    }

    public Checksum update(ByteBuffer buffer, int position, int size) {
        byte[] bytes = new byte[size];
        ((ByteBuffer) buffer.duplicate().position(position)).get(bytes);
            crc32.update( bytes);
        return this;
    }

}
