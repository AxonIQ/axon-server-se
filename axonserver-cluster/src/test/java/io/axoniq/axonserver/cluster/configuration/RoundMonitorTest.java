package io.axoniq.axonserver.cluster.configuration;

import io.axoniq.axonserver.cluster.configuration.wait.strategy.FastUpdateRound;
import io.axoniq.axonserver.cluster.configuration.wait.strategy.MultipleUpdateRound;
import io.axoniq.axonserver.cluster.configuration.wait.strategy.UpdateRound;
import io.axoniq.axonserver.cluster.exception.ServerTooSlowException;
import io.axoniq.axonserver.grpc.cluster.Node;
import org.junit.*;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;

/**
 * @author Sara Pellegrini
 */
public class RoundMonitorTest {

    private MultipleUpdateRound roundMonitor;

    @Test
    public void testSuccess() throws ExecutionException, InterruptedException {
        AtomicLong lastIndex = new AtomicLong(10L);
        AtomicLong currentTime = new AtomicLong(100L);
        roundMonitor = new MultipleUpdateRound(
                () -> 3,
                new FastUpdateRound(currentTime::get,
                                    () -> 300L,
                                    new UpdateRound(lastIndex::get,
                                                    consumer -> {
                                                        currentTime.addAndGet(100L);
                                                        consumer.accept(10L);
                                                        return () -> {};
                                                    })));

        CompletableFuture<Void> serverUpdated = roundMonitor.await();
        serverUpdated.get();
        assertTrue(serverUpdated.isDone());
    }

    @Test
    public void testFailure() {

        AtomicLong lastIndex = new AtomicLong(10L);
        AtomicLong currentTime = new AtomicLong(100L);
        roundMonitor = new MultipleUpdateRound(
                () -> 3,
                new FastUpdateRound(currentTime::get,
                                    () -> 300L,
                                    new UpdateRound(lastIndex::get,
                                                    consumer -> {
                                                        long prevLastIndex = lastIndex.getAndAdd(10L);
                                                        currentTime.addAndGet(1000L);
                                                        consumer.accept(prevLastIndex);
                                                        return () -> {};
                                                    })));
        CompletableFuture<Void> result = roundMonitor.await();
        try {
            result.get();
        } catch (Exception e) {
            assertTrue(result.isCompletedExceptionally());
            assertTrue(e.getCause() instanceof ServerTooSlowException);
        }
    }
}