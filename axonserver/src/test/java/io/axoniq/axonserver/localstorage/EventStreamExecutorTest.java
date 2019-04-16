/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage;

import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class EventStreamExecutorTest {

    private static final Logger logger = LoggerFactory.getLogger(EventStreamExecutor.class);
    private EventStreamExecutor testSubject = new EventStreamExecutor(3);

    @Test
    public void testTasksQueued() throws InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(10);
        long before = System.currentTimeMillis();
        IntStream.range(0, (int) countDownLatch.getCount())
                 .forEach(i ->
                                  testSubject.execute(() -> {
                                      try {
                                          logger.warn("Starting instance {}", i);
                                          Thread.sleep(100);
                                          countDownLatch.countDown();
                                      } catch (InterruptedException e) {
                                          e.printStackTrace();
                                      }
                                  }));
        countDownLatch.await(500, TimeUnit.MILLISECONDS);

        long after = System.currentTimeMillis();
        assertTrue(300 < after - before);
    }
}