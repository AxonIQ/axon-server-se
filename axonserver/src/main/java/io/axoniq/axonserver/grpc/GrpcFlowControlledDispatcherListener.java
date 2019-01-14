package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.message.FlowControlQueues;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

/**
 * Reads messages for a specific client from a queue and sends them to the client using gRPC.
 * Only reads messages when there are permits left.
 * @author Marc Gathier
 */
public abstract class GrpcFlowControlledDispatcherListener<I, T> {
    private static final Logger LOG = LoggerFactory.getLogger(GrpcFlowControlledDispatcherListener.class);
    private static final ExecutorService executorService = Executors.newCachedThreadPool(new CustomizableThreadFactory("request-dispatcher-"));

    protected final StreamObserver<I> inboundStream;
    private final AtomicLong permitsLeft = new AtomicLong(0);
    private final FlowControlQueues<T> queues;
    private final String queueName;
    private Future<?>[] futures;
    private volatile boolean running = true;

    public GrpcFlowControlledDispatcherListener(FlowControlQueues<T> queues, String queueName, StreamObserver<I> inboundStream, int threads) {
        this.queues = queues;
        this.queueName = queueName;
        this.inboundStream = inboundStream;
        futures = new Future[threads];
    }

    private void process() {
        try {
            getLogger().debug("Starting listener for {} ", queueName);
            while (running && permitsLeft.get() > 0) {
                getLogger().debug("waiting for message for {} ", queueName);
                T message = queues.take(queueName);
                if (message != null && send(message)) {
                    long left = permitsLeft.decrementAndGet();
                    getLogger().debug("{} permits left", left);
                }
            }
            getLogger().debug("Listener stopped as no more permits ({}) left for {} ", permitsLeft.get(), queueName);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            getLogger().info("Processing of messages from {} interrupted", queueName, e);
        }
    }

    /**
     * Sends a message to the connected client. If message was filtered at the processor return false.
     *
     * @param message the message to send
     * @return true if the message was sent to the client, false otherwise.
     */
    protected abstract boolean send(T message);


    public void addPermits(long count) {
        long old = permitsLeft.getAndAdd(count);
        getLogger().debug("Adding {} permits, #permits was: {}", count, old);
        if (old <= 0) {
            IntStream.range(0, futures.length).forEach(i -> {
                futures[i] = executorService.submit(this::process);
            });
        }
    }

    public void cancel() {
        permitsLeft.set(0);
        getLogger().debug("cancel listener for {} ", queueName);
        IntStream.range(0, futures.length).forEach(i -> {
            if( futures[i] != null) {
                futures[i].cancel(true);
            }
        });
        running = false;
    }


    public static void shutdown() {
        executorService.shutdown();
    }

    protected abstract Logger getLogger();
}
