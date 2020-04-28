/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.test;

import java.util.concurrent.TimeUnit;

/**
 * @author Marc Gathier
 */
public class AssertUtils {
    private AssertUtils() {

    }

    /**
     * Assert that the given {@code assertion} succeeds with the given {@code time} and {@code unit}.
     * @param time The time in which the assertion must pass
     * @param unit The unit in which time is expressed
     * @param assertion the assertion to succeed within the deadline
     */
    public static void assertWithin(int time, TimeUnit unit, Runnable assertion) throws InterruptedException {
        long now = System.currentTimeMillis();
        long deadline = now + unit.toMillis(time);
        do {
            try {
                assertion.run();
                break;
            } catch (AssertionError e) {
                if (now >= deadline) {
                    throw e;
                }
                Thread.sleep(10);
            }
            now = System.currentTimeMillis();
        } while (true);
    }


}
