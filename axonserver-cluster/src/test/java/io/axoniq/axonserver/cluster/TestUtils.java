package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.grpc.cluster.Entry;

import java.util.concurrent.TimeUnit;

public class TestUtils {

    private TestUtils() {
        // utility class
    }

    public static Entry newEntry(long term, long index) {
        return Entry.newBuilder()
                    .setTerm(term)
                    .setIndex(index)
                    .build();
    }

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
            }
            Thread.sleep(10);
            now = System.currentTimeMillis();
        } while (true);
    }
}
