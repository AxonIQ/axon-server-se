package io.axoniq.axonserver.cluster.util;

import java.util.concurrent.atomic.AtomicInteger;

public class ReplicatorPeerUtil {

    public static boolean isOverLimit(int serializedObjectSize, AtomicInteger currentMessageSize, int maxMessageSize,
                                       AtomicInteger currentNoOfMessages, int snapshotChunksBufferSize){
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
