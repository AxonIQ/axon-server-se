package io.axoniq.axonserver.message.command;

import io.axoniq.axonserver.message.command.hashing.ConsistentHashRoutingSelector;
import org.junit.*;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

/**
 * Unit test for {@link ConsistentHashRoutingSelector}
 *
 * @author Sara Pellegrini
 */
public class ConsistentHashRoutingSelectorTest {

    private int messagesNumber = 1_000_000;
    private double tolerance = 6;

    @Test
    public void testDistribution() {
        ConsistentHashRoutingSelector testSubject = new ConsistentHashRoutingSelector(Integer::parseInt);
        testSubject.register("5");
        testSubject.register("15");
        testSubject.register("30");
        testSubject.register("50");
        Map<String, AtomicInteger> counter = new HashMap<>();
        for (int i = 0; i < messagesNumber; i++) {
            String handler = testSubject.selectHandler(UUID.randomUUID().toString()).orElse(null);
            counter.computeIfAbsent(handler, h -> new AtomicInteger()).incrementAndGet();
        }
        counter.forEach((handler, count) -> {
            double percentage = count.get() * 100d / messagesNumber;
            assertEquals(Double.parseDouble(handler), percentage, tolerance);
        });
    }

    @Test
    public void testNoHandler() {
        ConsistentHashRoutingSelector testSubject = new ConsistentHashRoutingSelector(Integer::parseInt);
        for (int i = 0; i < 100; i++) {
            String handler = testSubject.selectHandler(UUID.randomUUID().toString()).orElse(null);
            assertNull(handler);
        }
    }

    @Test
    public void testSameRoutingKeySameHandler() {
        ConsistentHashRoutingSelector testSubject = new ConsistentHashRoutingSelector(Integer::parseInt);
        testSubject.register("5");
        testSubject.register("15");
        testSubject.register("30");
        testSubject.register("50");
        Map<String, AtomicInteger> counter = new HashMap<>();
        String routingKey = UUID.randomUUID().toString();
        for (int i = 0; i < messagesNumber; i++) {
            String handler = testSubject.selectHandler(routingKey).orElse(null);
            counter.computeIfAbsent(handler, h -> new AtomicInteger()).incrementAndGet();
        }
        assertEquals(1, counter.size());
        assertEquals(messagesNumber, counter.values().iterator().next().get());
    }

    @Test
    public void testUnregisteredHandler() {
        ConsistentHashRoutingSelector testSubject = new ConsistentHashRoutingSelector(Integer::parseInt);
        testSubject.register("20");
        testSubject.register("30");
        testSubject.register("50");
        Map<String, AtomicInteger> counter = new HashMap<>();
        for (int i = 0; i < messagesNumber; i++) {
            String handler = testSubject.selectHandler(UUID.randomUUID().toString()).orElse(null);
            counter.computeIfAbsent(handler, h -> new AtomicInteger()).incrementAndGet();
        }
        counter.forEach((handler, count) -> {
            double percentage = count.get() * 100d / messagesNumber;
            assertEquals(Double.parseDouble(handler), percentage, tolerance);
        });

        testSubject.unregister("50");
        counter.clear();
        for (int i = 0; i < messagesNumber; i++) {
            String handler = testSubject.selectHandler(UUID.randomUUID().toString()).orElse(null);
            counter.computeIfAbsent(handler, h -> new AtomicInteger()).incrementAndGet();
        }
        checkPercentage("20", 40, counter);
        checkPercentage("30", 60, counter);
    }

    private void checkPercentage(String handler, double expectedPercentage, Map<String, AtomicInteger> counter) {
        assertEquals(expectedPercentage, counter.get(handler).get() * 100d / messagesNumber, tolerance);
    }
}