package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axonserver.localstorage.SerializedTransactionWithToken;
import io.axoniq.axonserver.localstorage.TransactionInformation;

import org.springframework.data.util.CloseableIterator;

/**
 * @author Marc Gathier
 */
public interface TransactionIterator extends CloseableIterator<SerializedTransactionWithToken> {

    @Override
    default void close() {
        // Default no action, defined here to avoid IOException
    }

    TransactionInformation currentTransaction();
}
