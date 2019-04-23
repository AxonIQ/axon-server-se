package io.axoniq.axonserver.cluster.util;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Marc Gathier
 * @since 4.1
 */
public class AxonThreadFactory implements ThreadFactory {

    private final String prefix;
    private final AtomicInteger index = new AtomicInteger();

    public AxonThreadFactory(String prefix) {
        this.prefix = prefix;
    }

    @Override
    public Thread newThread( Runnable r) {
        Thread thread = new Thread(r);
        thread.setName(prefix + "-" + index.getAndDecrement());
        return thread;
    }
}
