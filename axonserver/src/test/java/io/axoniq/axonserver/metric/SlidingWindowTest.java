/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.metric;

import io.axoniq.axonserver.test.FakeClock;
import org.junit.*;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class SlidingWindowTest {

    private SlidingWindow<AtomicLong> testSubject;
    private FakeClock clock = new FakeClock();

    @Before
    public void init() {
        testSubject = new SlidingWindow<>(1, 60, TimeUnit.SECONDS, AtomicLong::new, (v1, v2) -> new AtomicLong(v1.longValue() + v2.longValue()), clock);
    }

    @Test
    public void current() {
        AtomicLong current = testSubject.current();
        current.incrementAndGet();
        current = testSubject.current();
        assertEquals(1, current.get());
        clock.timeElapses(5, TimeUnit.SECONDS);
        current = testSubject.current();
        assertEquals(0, current.get());
    }

    @Test
    public void bucket() {
        long now = clock.millis();
        long b1 = testSubject.bucket(now);
        long b2 = testSubject.bucket(now + TimeUnit.SECONDS.toMillis(5));
        assertEquals(b1+5, b2);
    }

    @Test
    public void reduce() {
        testSubject.current().incrementAndGet();
        clock.timeElapses(20, TimeUnit.SECONDS);
        assertEquals(1, testSubject.aggregate(1, TimeUnit.MINUTES).get());
        testSubject.current().incrementAndGet();
        assertEquals(2, testSubject.aggregate(1, TimeUnit.MINUTES).get());
        clock.timeElapses(41, TimeUnit.SECONDS);
        assertEquals(1, testSubject.aggregate(1, TimeUnit.MINUTES).get());
        clock.timeElapses(20, TimeUnit.SECONDS);
        assertEquals(0, testSubject.aggregate(1, TimeUnit.MINUTES).get());
    }
}