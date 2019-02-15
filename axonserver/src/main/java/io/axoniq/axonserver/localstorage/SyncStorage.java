package io.axoniq.axonserver.localstorage;

import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.localstorage.transaction.PreparedTransaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * @author Marc Gathier
 */
public class SyncStorage {
    private static final Logger logger = LoggerFactory.getLogger(SyncStorage.class);


    private final EventStore eventStore;

    public SyncStorage(EventStore eventStore) {
        this.eventStore = eventStore;
    }

    public void sync(long token, List<SerializedEvent> eventList) {
        if (token < eventStore.nextToken()) {
            logger.warn("{}: {} with token {} already stored",
                        eventStore.getType().getContext(),
                        eventStore.getType().getEventType(),
                        token);
            return;
        }

        if (token != eventStore.nextToken()) {
            logger.warn("{}: {} expecting token {} received {}",
                        eventStore.getType().getContext(), eventStore.getType().getEventType(),
                        eventStore.nextToken(),
                        token);
        }
        PreparedTransaction preparedTransaction = eventStore.prepareTransaction(eventList);
        try {
            eventStore.store(preparedTransaction).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new MessagingPlatformException(ErrorCode.DATAFILE_WRITE_ERROR, e.getMessage(), e);
        } catch (ExecutionException e) {
            throw new MessagingPlatformException(ErrorCode.DATAFILE_WRITE_ERROR, e.getMessage(), e.getCause());
        }
    }
}
