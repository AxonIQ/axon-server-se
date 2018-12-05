package io.axoniq.axonserver.cluster.configuration;

import io.axoniq.axonserver.cluster.exception.ServerTooSlowException;
import io.axoniq.axonserver.grpc.cluster.Node;
import org.junit.*;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static org.junit.Assert.*;

/**
 * @author Sara Pellegrini
 */
public class RoundMonitorTest {

    private RoundUpdateAlgorithm roundMonitor;

    @Test
    public void testSuccess() throws InterruptedException {
        AtomicLong lastIndex = new AtomicLong();
        AtomicLong currentTime = new AtomicLong();
        AtomicLong matchIndex = new AtomicLong();
        List<Consumer<Long>> matchIndexListener = new LinkedList<>();
        roundMonitor = new RoundUpdateAlgorithm(3,
                                                300,
                                                lastIndex::get,
                                                currentTime::get,
                                                (node, consumer) -> {
                                            matchIndexListener.add(consumer);
                                            consumer.accept(matchIndex.get());
                                            return () -> matchIndexListener.remove(consumer);
                                        });

        lastIndex.set(10L);
        currentTime.set(1000L);
        Node node = Node.newBuilder().build();
        CompletableFuture<Void> monitoring = CompletableFuture.runAsync(() -> roundMonitor.accept(node));
        Thread.sleep(100);
        lastIndex.set(20L);
        currentTime.set(2000L);
        matchIndex.set(10L);
        matchIndexListener.forEach(consumer -> consumer.accept(10L));
        Thread.sleep(100);
        assertFalse(monitoring.isDone());
        lastIndex.set(30L);
        currentTime.set(2100L);
        matchIndex.set(20L);
        matchIndexListener.forEach(consumer -> consumer.accept(20L));
        Thread.sleep(100);
        assertTrue(monitoring.isDone());
        assertFalse(monitoring.isCompletedExceptionally());
    }

    @Test(expected = ServerTooSlowException.class)
    public void testFailure() throws Throwable {
        AtomicLong lastIndex = new AtomicLong();
        AtomicLong currentTime = new AtomicLong();
        AtomicLong matchIndex = new AtomicLong();
        List<Consumer<Long>> matchIndexListener = new LinkedList<>();
        roundMonitor = new RoundUpdateAlgorithm(3,
                                                300,
                                                lastIndex::get,
                                                currentTime::get,
                                                (node, consumer) -> {
                                            matchIndexListener.add(consumer);
                                            consumer.accept(matchIndex.get());
                                            return () -> matchIndexListener.remove(consumer);
                                        });

        lastIndex.set(10L);
        currentTime.set(1000L);
        Node node = Node.newBuilder().build();
        CompletableFuture<Void> monitoring = CompletableFuture.runAsync(() -> roundMonitor.accept(node));
        Thread.sleep(100);
        lastIndex.set(20L);
        currentTime.set(2000L);
        matchIndex.set(10L);
        matchIndexListener.forEach(consumer -> consumer.accept(10L));
        Thread.sleep(100);
        assertFalse(monitoring.isDone());
        lastIndex.set(30L);
        currentTime.set(3000L);
        matchIndex.set(20L);
        matchIndexListener.forEach(consumer -> consumer.accept(20L));
        Thread.sleep(100);
        assertFalse(monitoring.isDone());
        lastIndex.set(40L);
        currentTime.set(4000L);
        matchIndex.set(30L);
        matchIndexListener.forEach(consumer -> consumer.accept(30L));
        Thread.sleep(100);
        assertTrue(monitoring.isCompletedExceptionally());
        try {
            monitoring.get();
        } catch (Exception e) {
            throw e.getCause();
        }
    }
}