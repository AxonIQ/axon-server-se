/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.util;

import io.axoniq.axonserver.test.FakeClock;
import org.junit.*;

import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class TimeLimitedCacheTest {

    private final FakeClock fakeClock = new FakeClock();
    private final TimeLimitedCache<String, String> testSubject = new TimeLimitedCache<>(fakeClock, 100);

    @Test
    public void put() {
        testSubject.put("Test", "Test123");
        Assert.assertEquals(1, testSubject.values().size());
    }

    @Test
    public void get() {
        testSubject.put("Test", "Test123");
        Assert.assertEquals("Test123", testSubject.get("Test"));
        fakeClock.timeElapses(101);
        Assert.assertNull(testSubject.get("Test"));
        Assert.assertEquals(0, testSubject.values().size());
    }

    @Test
    public void removeIf() {
        testSubject.put("Test", "Test123");
        Assert.assertEquals("Test123", testSubject.get("Test"));
        testSubject.removeIf((key, value) -> value.startsWith("Test"));
        Assert.assertNull(testSubject.get("Test"));
        Assert.assertEquals(0, testSubject.values().size());
    }
}