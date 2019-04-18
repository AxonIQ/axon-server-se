package io.axoniq.axonserver.cluster.configuration.wait.strategy;

import io.axoniq.axonserver.cluster.exception.ReplicationTimeoutException;
import org.junit.*;
import reactor.core.publisher.Flux;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link UpdateRound}
 *
 * @author Sara Pellegrini
 * @since 4.1.1
 */
public class UpdateRoundTest {


    @Test
    public void roundCompletes() throws Throwable{
        UpdateRound testSubject = new UpdateRound(() -> 100L, consumer ->
                Flux.range(1,100).map(Long::new).subscribe(consumer)::dispose);
        CompletableFuture<Void> future = testSubject.await();
        future.get(10, TimeUnit.SECONDS);
        assertFalse(future.isCompletedExceptionally());
    }

    @Test(expected = ReplicationTimeoutException.class)
    public void roundFailsIfReplicationIsNoMoreActive() throws Throwable {
        UpdateRound testSubject = new UpdateRound(() -> 100L, consumer -> () -> {});
        CompletableFuture<Void> future = testSubject.await();
        try {
            future.get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            assertTrue(future.isCompletedExceptionally());
            throw e.getCause();
        }
        fail();
    }



}