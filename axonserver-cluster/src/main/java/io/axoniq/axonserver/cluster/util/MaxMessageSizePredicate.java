package io.axoniq.axonserver.cluster.util;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

/**
 * Predicate which counts and checks if messages to be sent reach a configured limit
 * @author Greg Woods
 * @since 4.1.1
 */
public class MaxMessageSizePredicate implements Predicate<Integer> {

    private final AtomicInteger currentMessageSize = new AtomicInteger(0);
    private final AtomicInteger currentNoOfMessages = new AtomicInteger(0);
    private final int maxMessageSize;
    private final int snapshotChunksBufferSize;

    public MaxMessageSizePredicate(int maxMessageSize, int snapshotChunksBufferSize) {
        this.maxMessageSize = maxMessageSize;
        this.snapshotChunksBufferSize = snapshotChunksBufferSize;
    }

    @Override
    public boolean test(Integer serializedObjectSize) {
        if (serializedObjectSize + currentMessageSize.intValue() >= maxMessageSize ||
                currentNoOfMessages.intValue() + 1 > snapshotChunksBufferSize){

            currentMessageSize.set(serializedObjectSize); //prepare for next request
            currentNoOfMessages.set(1);
            return true;
        } else {
            currentMessageSize.set(currentMessageSize.get() + serializedObjectSize);
            currentNoOfMessages.incrementAndGet();
            return false;
        }
    }
}
