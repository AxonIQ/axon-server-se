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

    private static final byte[] BYTES = "this is a sample".getBytes();
    private static final ByteBuffer SAMPLE_BYTE_BUFFER = MappedByteBuffer.wrap(BYTES);
    private static final String WRONG_CHECKSUM = "The checksum value is calculated wrongly";

    @Test
    void testChecksum() {
        Checksum checksum = new Checksum();
        checksum.update(SAMPLE_BYTE_BUFFER, 0, BYTES.length);
        assertEquals(1895619624, checksum.get(), WRONG_CHECKSUM);
    }

    @Test
    void testLargeBuffer() {
        String string = IntStream.range(0, 100_000).mapToObj(i -> "" + i)
                                 .collect(Collectors.joining());
        byte[] bytes = string.getBytes();
        ByteBuffer byteBuffer = MappedByteBuffer.wrap(bytes);
        Checksum checksum = new Checksum();
        checksum.update(byteBuffer, 0, bytes.length);
        assertEquals(1739661821, checksum.get(), WRONG_CHECKSUM);
    }

    @Test
    void testSizeGreaterThanDataSize() {
        Checksum checksum = new Checksum();
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> {
            checksum.update(SAMPLE_BYTE_BUFFER, 0, BYTES.length + 1);
        }, "Size parameter greater than data should cause an IllegalArgumentException");
        assertEquals("The ByteBuffer is smaller than expected", e.getMessage());
    }

    @Test
    void testSizeLowerThanDataSize() {
        Checksum checksum = new Checksum();
        checksum.update(SAMPLE_BYTE_BUFFER, 0, BYTES.length - 6);
        assertEquals(2088123576, checksum.get(), WRONG_CHECKSUM);
    }

    @Test
    void testPositionGreaterThan0() {
        Checksum checksum = new Checksum();
        checksum.update(SAMPLE_BYTE_BUFFER, 5, BYTES.length - 5);
        assertEquals(1285876656, checksum.get(), WRONG_CHECKSUM);
    }

    @Test
    void testPositionLowerThan0() {
        Checksum checksum = new Checksum();
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> {
            checksum.update(SAMPLE_BYTE_BUFFER, -5, BYTES.length);
        }, "Position lower than 0 should cause an IllegalArgumentException");
        assertEquals("The position cannot be lower than 0", e.getMessage());
    }

    @Test
    void testPositionGreaterThan0andSizeGreaterThanRemainingDataSize() {
        Checksum checksum = new Checksum();
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> {
            checksum.update(SAMPLE_BYTE_BUFFER, 5, BYTES.length);
        }, "Size parameter greater than data should cause an IllegalArgumentException");
        assertEquals("The ByteBuffer is smaller than expected", e.getMessage());
    }
}