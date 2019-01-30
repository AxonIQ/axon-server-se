package io.axoniq.axonserver.localstorage;

import io.axoniq.axonserver.localstorage.transaction.StorageTransactionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.IntStream;

/**
 * @author Marc Gathier
 */
public class EventWriteStorage {
    private static final Logger logger = LoggerFactory.getLogger(EventWriteStorage.class);

    private final Map<String, Consumer<SerializedEventWithToken>> listeners = new ConcurrentHashMap<>();
    private final StorageTransactionManager storageTransactionManager;
    private final AtomicLong lastCommitted = new AtomicLong(-1L);


    public EventWriteStorage( StorageTransactionManager storageTransactionManager) {
        this.storageTransactionManager = storageTransactionManager;
    }

    public CompletableFuture<Void> store(List<SerializedEvent> eventList) {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        try {
            validate(eventList);

            storageTransactionManager.store(eventList).whenComplete((firstToken, cause) -> {
                if( cause == null) {
                    completableFuture.complete(null);
                    lastCommitted.set(firstToken + eventList.size() - 1);

                    if( ! listeners.isEmpty()) {
                        IntStream.range(0, eventList.size())
                                 .forEach(i -> {
                                     listeners.values()
                                              .forEach(consumer -> safeForwardEvent(consumer, new SerializedEventWithToken(firstToken + i, eventList.get(i))));
                                 });
                    }
                } else {
                    completableFuture.completeExceptionally(cause);
                }
            });
        } catch (RuntimeException cause) {
            completableFuture.completeExceptionally(cause);
        }
        return completableFuture;
    }

    private void safeForwardEvent(Consumer<SerializedEventWithToken> consumer, SerializedEventWithToken event) {
        try {
            consumer.accept(event);
        } catch( RuntimeException re) {
            logger.warn("Failed to forward event", re);
        }
    }

    public long getLastToken() {
        return storageTransactionManager.getLastToken();
    }
    public long getLastIndex() {
        return storageTransactionManager.getLastIndex();
    }

    public long getLastCommittedToken() {
        lastCommitted.compareAndSet(-1L, getLastToken());
        return lastCommitted.get();
    }

    private void validate(List<SerializedEvent> eventList) {
        storageTransactionManager.reserveSequenceNumbers(eventList);
    }

    public Registration registerEventListener(Consumer<SerializedEventWithToken> listener) {
        String id = UUID.randomUUID().toString();
        listeners.put(id, listener);
        return () -> listeners.remove(id);
    }


    public long waitingTransactions() {
        return storageTransactionManager.waitingTransactions();
    }

    public void rollback(long token) {
        storageTransactionManager.rollback(token);
    }

    public void cancelPendingTransactions() {
        storageTransactionManager.cancelPendingTransactions();
        storageTransactionManager.rollback(getLastCommittedToken());
    }
}
