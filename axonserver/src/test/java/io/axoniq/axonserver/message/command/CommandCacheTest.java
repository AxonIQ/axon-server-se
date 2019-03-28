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
import io.axoniq.axonserver.message.ClientIdentification;
import org.junit.*;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertNotNull;

/**
 * @author Marc Gathier
 */
public class CommandCacheTest {
    private CommandCache testSubject;
    private ChangeableClock clock;

    @Before
    public void setUp() {
        clock= new ChangeableClock();
        testSubject = new CommandCache(50000, clock);
    }

    @Test
    public void clearOnTimeout() {
        AtomicReference<SerializedCommandResponse> responseAtomicReference = new AtomicReference<>();
        testSubject.put("1234", new CommandInformation("1234", responseAtomicReference::set,
                                                       new ClientIdentification("context", "client"),
                                                       "component"));
        clock.forward(100000);
        testSubject.clearOnTimeout();
        assertNotNull(responseAtomicReference.get());

    }

    private class ChangeableClock extends Clock {
        Instant instant = Instant.now();

        @Override
        public ZoneId getZone() {
            return null;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return instant;
        }

        void forward( long millis) {
            instant = instant.plusMillis(millis);
        }
    }
}