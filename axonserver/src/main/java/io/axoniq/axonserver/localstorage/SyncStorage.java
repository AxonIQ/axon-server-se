package io.axoniq.axonserver.localstorage;

import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.localstorage.transaction.PreparedTransaction;

import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * @author Marc Gathier
 */
public class SyncStorage {


    private final EventStore datafileManagerChain;

    public SyncStorage(EventStore datafileManagerChain) {
        this.datafileManagerChain = datafileManagerChain;
    }

    public void sync(TransactionInformation transactionInformation, List<SerializedEvent> eventList) {
            PreparedTransaction preparedTransaction = datafileManagerChain.prepareTransaction(transactionInformation,
                                                                                              eventList);
            try {
                datafileManagerChain.store(preparedTransaction).get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new MessagingPlatformException(ErrorCode.DATAFILE_WRITE_ERROR, e.getMessage(), e);
            } catch (ExecutionException e) {
                throw new MessagingPlatformException(ErrorCode.DATAFILE_WRITE_ERROR, e.getMessage(), e.getCause());
            }
    }
}
