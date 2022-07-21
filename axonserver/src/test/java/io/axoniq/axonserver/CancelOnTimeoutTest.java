/*
 * Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver;

import io.axoniq.axonserver.test.FakeClock;
import org.junit.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link CancelOnTimeout} class.
 *
 * @author Marc Gathier
 * @author Sara Pellegrini
 */
public class CancelOnTimeoutTest {

    List<String> canceled = new ArrayList<>();
    FakeClock clock = new FakeClock();
    AtomicLong requestTimestamp = new AtomicLong(clock.millis());
    CancelOnTimeout<String> testSubject = new CancelOnTimeout<>("requestType",
                                                                clock,
                                                                10000,
                                                                Function.identity(),
                                                                s -> requestTimestamp.get(),
                                                                canceled::add);

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void cancel() {
        assertTrue(canceled.isEmpty());
        testSubject.cancel("request", "request");
        assertEquals(Collections.singletonList("request"), canceled);
    }

    @Test
    public void requestToBeCanceled() {
        assertFalse(testSubject.requestToBeCanceled("request", "request"));
        clock.timeElapses(10001);
        assertTrue(testSubject.requestToBeCanceled("request", "request"));
    }
}