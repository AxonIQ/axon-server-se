package io.axoniq.axonserver.enterprise;

import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * @author Marc Gathier
 */
public class CompetableFutureUtils {

    /**
     * Waits for a completable future to complete. Processes exception if completed exceptionally.
     * @param completableFuture the completable future
     * @param <T> the containing type
     * @return the completed value
     */
    public static <T> T getFuture(CompletableFuture<T> completableFuture) {
        try {
            return completableFuture.get();
        } catch (InterruptedException e) {
           Thread.currentThread().interrupt();
            throw new MessagingPlatformException(ErrorCode.OTHER, e.getMessage(), e);
        } catch (ExecutionException e) {
            if( e.getCause() instanceof MessagingPlatformException) {
                throw (MessagingPlatformException) e.getCause();
            }

            throw new MessagingPlatformException(ErrorCode.OTHER, e.getCause().getMessage(), e.getCause());
        }
    }
}
