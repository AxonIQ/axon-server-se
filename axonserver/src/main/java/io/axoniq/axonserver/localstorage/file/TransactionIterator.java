package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axonserver.grpc.internal.TransactionWithToken;
import org.springframework.data.util.CloseableIterator;

/**
 * Author: marc
 */
public interface TransactionIterator extends CloseableIterator<TransactionWithToken> {

    @Override
    default void close() {
        // Default no action, defined here to avoid IOException
    }
}
