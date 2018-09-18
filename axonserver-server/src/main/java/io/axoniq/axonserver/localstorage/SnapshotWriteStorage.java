package io.axoniq.axonserver.localstorage;

import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.Confirmation;
import io.axoniq.axonserver.localstorage.transaction.StorageTransactionManager;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Author: marc
 */
public class SnapshotWriteStorage {

    private final StorageTransactionManager storageTransactionManager;
    private final AtomicLong lastCommitted = new AtomicLong(-1);

    public SnapshotWriteStorage(StorageTransactionManager storageTransactionManager) {
        this.storageTransactionManager = storageTransactionManager;
    }

    public CompletableFuture<Confirmation> store(Event eventMessage) {
        CompletableFuture<Confirmation> completableFuture = new CompletableFuture<>();
        storageTransactionManager.store(Collections.singletonList(eventMessage), new StorageCallback() {
            @Override
            public boolean onCompleted(long firstToken) {
                completableFuture.complete(Confirmation.newBuilder().setSuccess(true).build());
                lastCommitted.set(firstToken);
                return true;
            }

            @Override
            public void onError(Throwable cause) {
                completableFuture.completeExceptionally(cause);
            }
        });
        return completableFuture;
    }

    public long getLastToken() {
        return storageTransactionManager.getLastToken();
    }

    public long waitingTransactions() {
        return storageTransactionManager.waitingTransactions();
    }

    public long getLastCommittedToken() {
        lastCommitted.compareAndSet(-1L, getLastToken());
        return lastCommitted.get();
    }

    public void rollback(long token) {
        storageTransactionManager.rollback(token);
    }
}
