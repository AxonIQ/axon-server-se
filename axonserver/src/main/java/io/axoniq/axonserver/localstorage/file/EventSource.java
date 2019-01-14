package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axonserver.localstorage.SerializedEvent;

/**
 * @author Marc Gathier
 */
public interface EventSource extends AutoCloseable {

    SerializedEvent readEvent(int position);

    default void close()  {
        // no-action
    }

    TransactionIterator createTransactionIterator(long segment, long token, boolean validating);

    EventIterator createEventIterator(long segment, long startToken);
}
