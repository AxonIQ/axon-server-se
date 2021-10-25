/*
 *  Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.message.FlowControlQueues;
import io.axoniq.axonserver.test.FakeStreamObserver;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 * @since 4.5.8
 */
public class GrpcFlowControlledDispatcherListenerTest {
    private GrpcFlowControlledDispatcherListener<String,String> testSubject;

    @Before
    public void setUp() throws Exception {
        FlowControlQueues<String> queues = new FlowControlQueues<>();
        testSubject = new GrpcFlowControlledDispatcherListener<String,String>(queues, "queue1", new FakeStreamObserver<String>(), 1) {
            private final Logger logger = LoggerFactory.getLogger("test");
            @Override
            protected boolean send(String message) {
                return false;
            }

            @Override
            protected Logger getLogger() {
                return logger;
            }
        };
    }

    @Test
    public void waiting() {
        assertEquals(0, testSubject.waiting());
    }
}