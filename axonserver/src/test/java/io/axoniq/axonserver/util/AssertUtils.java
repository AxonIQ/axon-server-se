package io.axoniq.axonserver.util;

import java.util.concurrent.TimeUnit;

/**
 * Author: marc
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
