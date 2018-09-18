package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axonserver.grpc.event.Event;

/**
 * Author: marc
 */
public interface EventSource extends AutoCloseable {

    Event readEvent(int position);

    default void close()  {
        // no-action
    }

    TransactionIterator createTransactionIterator(long segment, long token, boolean validating);

    EventIterator createEventIterator(long segment, long startToken);
}
