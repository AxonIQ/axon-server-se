package io.axoniq.axonserver.cluster.configuration;

import io.axoniq.axonserver.cluster.configuration.wait.strategy.FastUpdateRound;
import io.axoniq.axonserver.cluster.configuration.wait.strategy.MultipleUpdateRound;
import io.axoniq.axonserver.cluster.configuration.wait.strategy.UpdateRound;
import io.axoniq.axonserver.cluster.exception.ServerTooSlowException;
import org.junit.*;
import reactor.core.publisher.EmitterProcessor;

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
        AtomicLong currentTime = new AtomicLong(100L);
        EmitterProcessor<Long> processor = EmitterProcessor.create(1);
        UpdateRound updateRound = new UpdateRound(() -> 10L, processor.replay().autoConnect());
        FastUpdateRound fastUpdateRound = new FastUpdateRound(currentTime::get, () -> 300L, updateRound);
        roundMonitor = new MultipleUpdateRound(() -> 3, fastUpdateRound);

        CompletableFuture<Void> serverUpdated = roundMonitor.await();
        currentTime.addAndGet(200L);
        processor.sink().next(10L);
        serverUpdated.get();
        assertTrue(serverUpdated.isDone());
    }

    @Test
    public void testFailure() {

        AtomicLong currentTime = new AtomicLong(100L);
        EmitterProcessor<Long> processor = EmitterProcessor.create(1);
        UpdateRound updateRound = new UpdateRound(() -> 10L, processor.replay().autoConnect());
        FastUpdateRound fastUpdateRound = new FastUpdateRound(currentTime::get, () -> 300L, updateRound);
        roundMonitor = new MultipleUpdateRound(() -> 3, fastUpdateRound);

        CompletableFuture<Void> serverUpdated = roundMonitor.await();
        currentTime.addAndGet(500L);
        processor.sink().next(10L);
        try {
            serverUpdated.get();
        } catch (Exception e) {
            assertTrue(serverUpdated.isCompletedExceptionally());
            assertTrue(e.getCause() instanceof ServerTooSlowException);
        }
    }
}