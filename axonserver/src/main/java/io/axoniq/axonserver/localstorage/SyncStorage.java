package io.axoniq.axonserver.localstorage;

import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.localstorage.transaction.PreparedTransaction;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Author: marc
 */
public class SyncStorage {


    private final EventStore datafileManagerChain;

    public SyncStorage(EventStore datafileManagerChain) {
        this.datafileManagerChain = datafileManagerChain;
    }

    public void sync(List<Event> eventList) {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        PreparedTransaction preparedTransaction = datafileManagerChain.prepareTransaction(eventList);
        datafileManagerChain.store(preparedTransaction, new StorageCallback() {

            @Override
            public boolean onCompleted(long firstToken) {
                completableFuture.complete(null);
                return true;
            }

            @Override
            public void onError(Throwable cause) {
                completableFuture.completeExceptionally(cause);
            }
        });

        try {
            completableFuture.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new MessagingPlatformException(ErrorCode.DATAFILE_WRITE_ERROR, e.getMessage(), e);
        } catch (ExecutionException e) {
            throw new MessagingPlatformException(ErrorCode.DATAFILE_WRITE_ERROR, e.getMessage(), e.getCause());
        }
    }
}
