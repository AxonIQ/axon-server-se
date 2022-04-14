/*
 * Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver;

import io.axoniq.axonserver.message.command.InsufficientBufferCapacityException;
import org.junit.*;

import static org.junit.Assert.*;

/**
 * Unit test for {@link LimitedBuffer}.
 *
 * @author Marc Gathier
 * @author Sara Pellegrini
 */
public class LimitedBufferTest {

    @Test
    public void size() {
        LimitedBuffer<String> limitedBuffer = new LimitedBuffer<>("bufferName", "Error", 2);
        limitedBuffer.put("key1", "string1");
        assertEquals(1, limitedBuffer.size());
        limitedBuffer.put("key2", "string2");
        assertEquals(2, limitedBuffer.size());
        try {
            limitedBuffer.put("key3", "string3");
        } catch (InsufficientBufferCapacityException e) {
            //do nothing
        }
        assertEquals(2, limitedBuffer.size());
    }

    @Test
    public void remove() {
        LimitedBuffer<String> limitedBuffer = new LimitedBuffer<>("bufferName", "Error", 2);
        limitedBuffer.put("key1", "string1");
        limitedBuffer.put("key2", "string2");
        String string1 = limitedBuffer.remove("key1");
        assertEquals("string1", string1);
        assertEquals("string2", limitedBuffer.get("key2"));
        assertNull(limitedBuffer.get("key1"));
    }

    @Test
    public void putWithSpace() {
        LimitedBuffer<String> limitedBuffer = new LimitedBuffer<>("bufferName", "Error", 3);
        limitedBuffer.put("key", "aString");
        assertEquals("aString", limitedBuffer.get("key"));
    }

    @Test(expected = InsufficientBufferCapacityException.class)
    public void putWithoutSpace() {
        LimitedBuffer<String> limitedBuffer = new LimitedBuffer<>("bufferName", "Error", 2);
        limitedBuffer.put("key1", "string1");
        limitedBuffer.put("key2", "string2");
        limitedBuffer.put("key3", "string3");
    }
}