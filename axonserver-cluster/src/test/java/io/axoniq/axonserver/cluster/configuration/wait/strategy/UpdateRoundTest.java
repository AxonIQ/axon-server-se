package io.axoniq.axonserver.cluster.configuration.wait.strategy;

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
        UpdateRound testSubject = new UpdateRound(() -> 100L, Flux.range(1, 100).map(Long::new));
        CompletableFuture<Void> future = testSubject.await();
        future.get(10, TimeUnit.SECONDS);
        assertFalse(future.isCompletedExceptionally());
    }


}