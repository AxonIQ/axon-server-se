package io.axoniq.axonhub.localstorage.file;

import io.axoniq.axonhub.internal.grpc.TransactionWithToken;

import java.util.Iterator;

/**
 * Author: marc
 */
public interface TransactionIterator extends Iterator<TransactionWithToken>, AutoCloseable {

    @Override
    default void close() {
        // Default no action, defined here to avoid IOException
    }
}
