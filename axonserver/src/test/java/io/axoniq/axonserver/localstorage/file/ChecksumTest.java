/*
 * Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.file;

import org.junit.jupiter.api.*;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link Checksum}.
 *
 * @author Sara Pellegrini
 * @author Milan Savic
 */
class ChecksumTest {

    @Test
    void testChecksum() {
        String string = "this is a sample";
        byte[] bytes = string.getBytes();
        ByteBuffer byteBuffer = MappedByteBuffer.wrap(bytes);
        Checksum checksum = new Checksum();
        checksum.update(byteBuffer, 0, bytes.length);
        int i = checksum.get();
        assertEquals(1895619624, i);
    }

    @Test
    void testLargeBuffer() {
        String string = IntStream.range(0, 100_000).mapToObj(i -> "" + i)
                                 .collect(Collectors.joining());
        byte[] bytes = string.getBytes();
        ByteBuffer byteBuffer = MappedByteBuffer.wrap(bytes);
        Checksum checksum = new Checksum();
        checksum.update(byteBuffer, 0, bytes.length);
        int i = checksum.get();
        assertEquals(1739661821, i);
    }
}