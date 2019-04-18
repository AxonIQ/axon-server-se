package io.axoniq.axonserver.cluster.configuration.wait.strategy;

import io.axoniq.axonserver.cluster.configuration.WaitStrategy;
import io.axoniq.axonserver.cluster.exception.ServerTooSlowException;
import org.junit.*;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link MultipleUpdateRound}
 *
 * @author Sara Pellegrini
 * @since 4.1.1
 */
public class MultipleUpdateRoundTest {

    @Test
    public void completesSuccessfully() throws Throwable{
        AtomicInteger count = new AtomicInteger(0);
        WaitStrategy completesAtThirdRound = () -> {
            int roundNumber = count.incrementAndGet();
            if (roundNumber >= 3){
                return CompletableFuture.completedFuture(null);
            }
            CompletableFuture failed = new CompletableFuture();
            failed.completeExceptionally(new ServerTooSlowException(""));
            return failed;
        };
        MultipleUpdateRound testSubject = new MultipleUpdateRound(() -> 10, completesAtThirdRound);
        CompletableFuture<Void> future = testSubject.await();
        future.get();
        assertFalse(future.isCompletedExceptionally());
    }

    @Test(expected = ServerTooSlowException.class)
    public void failsBecauseServerIsTooSlow() throws Throwable{
        WaitStrategy tooSlow = () -> {
            CompletableFuture failed = new CompletableFuture();
            failed.completeExceptionally(new ServerTooSlowException(""));
            return failed;
        };
        MultipleUpdateRound testSubject = new MultipleUpdateRound(() -> 10, tooSlow);
        CompletableFuture<Void> future = testSubject.await();
        try {
            future.get();
        } catch (Exception e) {
            assertTrue(future.isCompletedExceptionally());
            throw e.getCause();
        }
    }

    @Test(expected = CustomException.class)
    public void failsBecauseUnknownError()  throws Throwable{
        WaitStrategy tooSlow = () -> {
            CompletableFuture failed = new CompletableFuture();
            failed.completeExceptionally(new CustomException());
            return failed;
        };
        MultipleUpdateRound testSubject = new MultipleUpdateRound(() -> 10, tooSlow);
        CompletableFuture<Void> future = testSubject.await();
        try {
            future.get();
        } catch (Exception e) {
            assertTrue(future.isCompletedExceptionally());
            throw e.getCause();
        }
    }


    private static class CustomException extends RuntimeException {

    }

}