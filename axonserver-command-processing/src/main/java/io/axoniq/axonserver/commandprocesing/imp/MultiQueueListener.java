package io.axoniq.axonserver.commandprocesing.imp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class MultiQueueListener<T> {

    private final static Logger logger = LoggerFactory.getLogger(MultiQueueListener.class);
    private final static AtomicInteger NO_REQUESTS = new AtomicInteger();
    private final Map<String, BlockingQueue<T>> queues = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> remainingRequests = new ConcurrentHashMap<>();


    public void addQueue(String queue, int initialRequests) {
        queues.put(queue, new LinkedBlockingQueue<>());
        remainingRequests.put(queue, new AtomicInteger(initialRequests));
    }

    public void enqueue(String queue, T message) {
        queues.get(queue).offer(message);
        synchronized (queues) {
            queues.notify();
            logger.debug("notified");
        }
    }

    public void request(String queue, int count) {
        remainingRequests.get(queue).addAndGet(count);
        synchronized (queues) {
            queues.notify();
            logger.debug("notified");
        }
    }

    public void sender() {
        try {
            checkAndSendMessages();
            while (true) {
                synchronized (queues) {
                    logger.debug("Waiting for notification");
                    queues.wait();
                }
                logger.debug("received notification");
                checkAndSendMessages();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void checkAndSendMessages() {
        queues.forEach((queueName, queue) -> {
            while (queue.size() > 0 && remainingRequests.getOrDefault(queueName, NO_REQUESTS).get() > 0) {
                T message = queue.poll();
                remainingRequests.get(queueName).decrementAndGet();
                logger.debug("{} -> {}", message, queueName);
            }
        });
    }
}
