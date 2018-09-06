package io.axoniq.axonhub.component.processor.balancing;

import io.axoniq.axonhub.EventProcessorEvents.EventProcessorStatusUpdated;
import io.axoniq.axonhub.EventProcessorEvents.ProcessorStatusRequest;
import io.axoniq.axonhub.component.instance.Clients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Consumer;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Created by Sara Pellegrini on 29/08/2018.
 * sara.pellegrini@gmail.com
 */
@Component
public class UpdatedLoadBalance {

    private final Logger logger = LoggerFactory.getLogger(UpdatedLoadBalance.class);

    private final Clients clients;

    private final LoadBalancingStrategy delegate;

    private final ApplicationEventPublisher eventPublisher;

    private final Map<TrackingEventProcessor, ExecutorService> executors = new HashMap<>();

    private final List<Consumer<EventProcessorStatusUpdated>> updateListeners = new CopyOnWriteArrayList<>();

    public UpdatedLoadBalance(Clients clients,
                              LoadBalancingStrategy delegate,
                              ApplicationEventPublisher eventPublisher) {
        this.clients = clients;
        this.delegate = delegate;
        this.eventPublisher = eventPublisher;
    }

    @EventListener
    public void on(EventProcessorStatusUpdated update) {
        updateListeners.forEach(c -> c.accept(update));
    }

    public void balance(TrackingEventProcessor processor) {
        ExecutorService service = executors.computeIfAbsent(processor, p -> {
            return new ThreadPoolExecutor(0,1,1,SECONDS,new LinkedBlockingQueue<>());
        });
        service.execute(() -> {
            try {
                Thread.sleep(15000);
                Set<String> attendedInfo = new HashSet<>();
                clients.forEach(client -> attendedInfo.add(client.name()));
                CountDownLatch count = new CountDownLatch(attendedInfo.size());
                Consumer<EventProcessorStatusUpdated> consumer = status -> {
                    String client = status.eventProcessorStatus().getClient();
                    boolean removed = attendedInfo.remove(client);
                    if (removed) {
                        count.countDown();
                    }
                };
                updateListeners.add(consumer);
                clients.forEach(client -> eventPublisher.publishEvent(new ProcessorStatusRequest(client.name(), processor.name(), false)));
                boolean updated = count.await(10, SECONDS);
                updateListeners.remove(consumer);
                if (updated){
                    delegate.balance(processor).perform();
                }
            } catch (InterruptedException e) {
                logger.warn("Thread interrupted during Load Balancing Operation", e);
                Thread.currentThread().interrupt();
            }
        });
    }

}