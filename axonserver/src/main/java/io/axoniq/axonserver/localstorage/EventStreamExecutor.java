package io.axoniq.axonserver.localstorage;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.Executor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Thread pool used by event streaming operations. limits the number of threads available for streaming older events.
 *
 * @author Marc Gathier
 */
@Component
public class EventStreamExecutor {
    private final Executor eventStreamExecutor;

    public EventStreamExecutor(@Value("${axon.axonserver.event-stream-threads:8}") int maxThreads) {
        eventStreamExecutor = new ThreadPoolExecutor(0, maxThreads, 60L, TimeUnit.SECONDS,
                                                     new SynchronousQueue<>(),
                                                     new CustomizableThreadFactory("event-stream-"){
            @Override
            public Thread newThread(Runnable runnable) {
                Thread thread = super.newThread(runnable);
                thread.setDaemon(true);
                return thread;
            }
        });
    }

    public void execute(Runnable task) {
        eventStreamExecutor.execute(task);
    }
}
