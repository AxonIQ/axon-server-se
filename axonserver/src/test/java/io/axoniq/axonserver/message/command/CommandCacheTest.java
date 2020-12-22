/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.command;

import io.axoniq.axonserver.grpc.SerializedCommandResponse;
import io.axoniq.axonserver.message.ClientStreamIdentification;
import io.axoniq.axonserver.test.FakeClock;
import org.junit.*;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class CommandCacheTest {

    private CommandCache testSubject;
    private FakeClock clock;

    @Before
    public void setUp() {
        clock = new FakeClock();
        testSubject = new CommandCache(50000, clock,1);
    }

    @Test
    public void clearOnTimeout() {
        AtomicReference<SerializedCommandResponse> responseAtomicReference = new AtomicReference<>();
        testSubject.put("1234", new CommandInformation("1234",
                                                       "Source",
                                                       "Target",
                                                       responseAtomicReference::set,
                                                       new ClientStreamIdentification("context", "client"),
                                                       "component"));
        clock.timeElapses(100000);
        testSubject.clearOnTimeout();
        assertNotNull(responseAtomicReference.get());

    }

    @Test(expected = InsufficientBufferCapacityException.class)
    public void onFullCapacityThrowError() {
        AtomicReference<SerializedCommandResponse> responseAtomicReference = new AtomicReference<>();

        testSubject.put("1234", new CommandInformation("1234", "Source", "Target", responseAtomicReference::set,
                                                       new ClientStreamIdentification("context", "client"),
                                                       "component"));


        testSubject.put("4567", new CommandInformation("4567", "Source", "Target", responseAtomicReference::set,
                                                       new ClientStreamIdentification("context", "client"),
                                                       "component"));
    }
}