package io.axoniq.axonserver.message.command;

import org.junit.*;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

/**
 * Unit test for {@link LoadFactorBasedSelector}
 *
 * @author Sara Pellegrini
 */
public class LoadFactorBasedSelectorTest {

    @Test
    public void testDistribution() {
        LoadFactorBasedSelector<Integer> testSubject = new LoadFactorBasedSelector<>(i -> i);
        testSubject.register(5);
        testSubject.register(15);
        testSubject.register(25);
        testSubject.register(45);
        Map<Integer, AtomicInteger> counter = new HashMap<>();
        for (int i = 0; i < 1_000_000; i++) {
            Integer handler = testSubject.selectHandler(UUID.randomUUID().toString()).orElse(null);
            counter.computeIfAbsent(handler, h -> new AtomicInteger()).incrementAndGet();
        }
        counter.forEach((handler, count) -> assertEquals(handler.doubleValue(), count.get() * 100 / 1_000_000d, 6));
    }

    @Test
    public void testNoHandler() {
        LoadFactorBasedSelector<Integer> testSubject = new LoadFactorBasedSelector<>(i -> i);
        for (int i = 0; i < 100; i++) {
            Integer handler = testSubject.selectHandler(UUID.randomUUID().toString()).orElse(null);
            assertNull(handler);
        }
    }

    @Test
    public void testSameRoutingKeySameHandler() {
        LoadFactorBasedSelector<Integer> testSubject = new LoadFactorBasedSelector<>(i -> i);
        testSubject.register(5);
        testSubject.register(15);
        testSubject.register(25);
        testSubject.register(45);
        Map<Integer, AtomicInteger> counter = new HashMap<>();
        Integer handler = null;
        String routingKey = UUID.randomUUID().toString();
        for (int i = 0; i < 1_000_000; i++) {
            handler = testSubject.selectHandler(routingKey).orElse(null);
            counter.computeIfAbsent(handler, h -> new AtomicInteger()).incrementAndGet();
        }
        for (Map.Entry<Integer, AtomicInteger> entry : counter.entrySet()) {
            if (entry.getKey().equals(handler)) {
                assertEquals(1_000_000, entry.getValue().get());
            } else {
                assertEquals(0, entry.getValue().get());
            }
        }
    }

    @Test
    public void testUnregisteredHandler() {
        LoadFactorBasedSelector<Integer> testSubject = new LoadFactorBasedSelector<>(i -> i);
        testSubject.register(20);
        testSubject.register(30);
        testSubject.register(50);
        Map<Integer, AtomicInteger> counter = new HashMap<>();
        for (int i = 0; i < 1_000_000; i++) {
            Integer handler = testSubject.selectHandler(UUID.randomUUID().toString()).orElse(null);
            counter.computeIfAbsent(handler, h -> new AtomicInteger()).incrementAndGet();
        }
        counter.forEach((handler, count) -> assertEquals(handler.doubleValue(), count.get() * 100 / 1_000_000d, 6));

        testSubject.unregister(50);
        counter.clear();
        for (int i = 0; i < 1_000_000; i++) {
            Integer handler = testSubject.selectHandler(UUID.randomUUID().toString()).orElse(null);
            counter.computeIfAbsent(handler, h -> new AtomicInteger()).incrementAndGet();
        }

        assertEquals(40, counter.get(20).get() * 100 / 1_000_000d, 6);
        assertEquals(60, counter.get(30).get() * 100 / 1_000_000d, 6);
    }
}