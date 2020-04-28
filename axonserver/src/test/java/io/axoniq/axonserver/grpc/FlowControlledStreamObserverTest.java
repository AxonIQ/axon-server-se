/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.test.FakeStreamObserver;
import org.junit.*;

import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class FlowControlledStreamObserverTest {
    private FlowControlledStreamObserver<String> testSubject;
    private AtomicReference<Throwable> errorReference = new AtomicReference<>();
    private FakeStreamObserver<String> delegate = new FakeStreamObserver<>();

    @Before
    public void setUp() throws Exception {
        testSubject = new FlowControlledStreamObserver<>(delegate, t -> errorReference.set(t));
        testSubject.addPermits(10);
    }

    @Test
    public void testNoMorePermits() {
        IntStream.range(0, 20).forEach(i -> testSubject.onNext("Sample"));
        assertEquals(10, delegate.values().size());
        assertEquals(IllegalStateException.class, errorReference.get().getClass());
        testSubject.addPermits(10);
        IntStream.range(0, 20).forEach(i -> testSubject.onNext("Sample"));
        assertEquals(20, delegate.values().size());
    }
}