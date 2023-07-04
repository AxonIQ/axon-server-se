/*
 *  Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.message.ClientStreamIdentification;
import io.axoniq.axonserver.message.FlowControlQueues;
import io.axoniq.axonserver.test.FakeStreamObserver;
import io.axoniq.axonserver.topology.Topology;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

/**
 * @author Marc Gathier
 * @since 4.5.8
 */
public class GrpcFlowControlledDispatcherListenerTest {

    private final Logger logger = LoggerFactory.getLogger("test");
    private GrpcFlowControlledDispatcherListener<String, String> testSubject;

    @Before
    public void setUp() throws Exception {
        FlowControlQueues<String> queues = new FlowControlQueues<>();
        testSubject = new GrpcFlowControlledDispatcherListener<>(queues,
                                                                 new ClientStreamIdentification(
                                                                         Topology.DEFAULT_CONTEXT,
                                                                         "queue1"),
                                                                 new FakeStreamObserver<>(),
                                                                 1) {
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

    @Test
    public void testExceptionOccurredSendingNextInstruction() throws InterruptedException {
        FlowControlQueues<String> queues = new FlowControlQueues<>();
        ClientStreamIdentification myQueueName = new ClientStreamIdentification(Topology.DEFAULT_CONTEXT,
                                                                                "MyQueueName");
        queues.put(myQueueName, "One");
        queues.put(myQueueName, "Two");
        queues.put(myQueueName, "Three");
        queues.put(myQueueName, "specialOne");
        queues.put(myQueueName, "Four");
        queues.put(myQueueName, "specialOne");
        queues.put(myQueueName, "Five");
        queues.put(myQueueName, "specialOne");
        queues.put(myQueueName, "Six");
        queues.put(myQueueName, "Final");
        CountDownLatch countDownLatch = new CountDownLatch(6);
        GrpcFlowControlledDispatcherListener<String, String> listener =
                new GrpcFlowControlledDispatcherListener<>(queues, myQueueName, new FakeStreamObserver<>(), 2) {

                    @Override
                    protected boolean send(String message) {
                        logger.warn(Thread.currentThread().getName());
                        if (message.equals("specialOne")) {
                            throw new RuntimeException();
                        }
                        logger.warn(message);

                        countDownLatch.countDown();
                        return true;
                    }

                    @Override
                    protected Logger getLogger() {
                        return logger;
                    }
                };
        listener.addPermits(10);
        countDownLatch.await(1, TimeUnit.SECONDS);
        assertEquals(0, countDownLatch.getCount());
    }

}