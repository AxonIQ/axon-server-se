package io.axoniq.axonserver.message.command;

import io.axoniq.axonserver.message.command.hashing.ConsistentHashRoutingSelector;
import org.junit.*;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

/**
 * Unit test for {@link io.axoniq.axonserver.message.command.hashing.ConsistentHashRoutingSelector}
 *
 * @author Sara Pellegrini
 */
public class LoadFactorBasedSelectorTest {

    @Test
    public void testDistribution() {
        ConsistentHashRoutingSelector testSubject = new ConsistentHashRoutingSelector(Integer::parseInt);
        testSubject.register("5");
        testSubject.register("15");
        testSubject.register("30");
        testSubject.register("50");
        Map<String, AtomicInteger> counter = new HashMap<>();
        int messages = 1_000_000;
        for (int i = 0; i < messages; i++) {
            String handler = testSubject.selectHandler(UUID.randomUUID().toString()).orElse(null);
            counter.computeIfAbsent(handler, h -> new AtomicInteger()).incrementAndGet();
        }
        System.out.println(counter);
        counter.forEach((handler, count) -> assertEquals(Double.parseDouble(handler),
                                                         count.get() * 100d / messages,
                                                         6));
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
        String handler = null;
        String routingKey = UUID.randomUUID().toString();
        for (int i = 0; i < 1_000_000; i++) {
            handler = testSubject.selectHandler(routingKey).orElse(null);
            counter.computeIfAbsent(handler, h -> new AtomicInteger()).incrementAndGet();
        }
        for (Map.Entry<String, AtomicInteger> entry : counter.entrySet()) {
            if (entry.getKey().equals(handler)) {
                assertEquals(1_000_000, entry.getValue().get());
            } else {
                assertEquals(0, entry.getValue().get());
            }
        }
    }

    @Test
    public void testUnregisteredHandler() {
        ConsistentHashRoutingSelector testSubject = new ConsistentHashRoutingSelector(Integer::parseInt);
        testSubject.register("20");
        testSubject.register("30");
        testSubject.register("50");
        Map<String, AtomicInteger> counter = new HashMap<>();
        for (int i = 0; i < 1_000_000; i++) {
            String handler = testSubject.selectHandler(UUID.randomUUID().toString()).orElse(null);
            counter.computeIfAbsent(handler, h -> new AtomicInteger()).incrementAndGet();
        }
        counter.forEach((handler, count) -> assertEquals(Double.parseDouble(handler),
                                                         count.get() * 100 / 1_000_000d,
                                                         6));

        testSubject.unregister("50");
        counter.clear();
        for (int i = 0; i < 1_000_000; i++) {
            String handler = testSubject.selectHandler(UUID.randomUUID().toString()).orElse(null);
            counter.computeIfAbsent(handler, h -> new AtomicInteger()).incrementAndGet();
        }

        assertEquals(40, counter.get("20").get() * 100 / 1_000_000d, 6);
        assertEquals(60, counter.get("30").get() * 100 / 1_000_000d, 6);
    }
}