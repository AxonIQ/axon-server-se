package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axonserver.grpc.internal.TransactionWithToken;

import java.util.Iterator;

/**
 * @author Marc Gathier
 */
public interface TransactionIterator extends Iterator<TransactionWithToken>, AutoCloseable {

    @Override
    default void close() {
        // Default no action, defined here to avoid IOException
    }
}
