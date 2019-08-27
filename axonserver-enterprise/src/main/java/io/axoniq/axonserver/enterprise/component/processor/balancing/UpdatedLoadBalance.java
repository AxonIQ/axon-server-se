package io.axoniq.axonserver.enterprise.component.processor.balancing;

import io.axoniq.axonserver.applicationevents.EventProcessorEvents.EventProcessorStatusUpdated;
import io.axoniq.axonserver.applicationevents.EventProcessorEvents.ProcessorStatusRequest;
import io.axoniq.axonserver.component.processor.balancing.TrackingEventProcessor;
import io.axoniq.axonserver.component.processor.balancing.strategy.ProcessorLoadBalanceStrategy;
import io.axoniq.axonserver.component.processor.listener.ClientProcessor;
import io.axoniq.axonserver.component.processor.listener.ClientProcessors;
import io.axoniq.axonserver.enterprise.component.processor.balancing.jpa.RaftProcessorLoadBalancing;
import io.axoniq.axonserver.enterprise.component.processor.balancing.stategy.RaftProcessorLoadBalancingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Created by Sara Pellegrini on 29/08/2018.
 * sara.pellegrini@gmail.com
 */
@Component
public class UpdatedLoadBalance {

    private final Logger logger = LoggerFactory.getLogger(UpdatedLoadBalance.class);

    private static final int BALANCING_TRIGGER_MILLIS = 15000;
    private static final String DEFAULT_STRATEGY_NAME = "default";

    private final ClientProcessors allClientProcessors;
    private final ApplicationEventPublisher eventPublisher;
    private final ProcessorLoadBalanceStrategy loadBalancingStrategy;
    private final RaftProcessorLoadBalancingService loadBalancingService;

    private final Map<TrackingEventProcessor, ExecutorService> executors = new HashMap<>();
    private final List<Consumer<EventProcessorStatusUpdated>> updateListeners = new CopyOnWriteArrayList<>();

    public UpdatedLoadBalance(ClientProcessors allClientProcessors,
                              ApplicationEventPublisher eventPublisher,
                              ProcessorLoadBalanceStrategy loadBalancingStrategy,
                              RaftProcessorLoadBalancingService loadBalancingService) {
        this.allClientProcessors = allClientProcessors;
        this.eventPublisher = eventPublisher;
        this.loadBalancingStrategy = loadBalancingStrategy;
        this.loadBalancingService = loadBalancingService;
    }

    @EventListener
    public void on(EventProcessorStatusUpdated event) {
        updateListeners.forEach(listener -> listener.accept(event));
    }

    public void balance(TrackingEventProcessor trackingProcessor) {
        ExecutorService service = executors.computeIfAbsent(
                trackingProcessor, tep -> new ThreadPoolExecutor(0, 1, 1, SECONDS, new LinkedBlockingQueue<>())
        );

        service.execute(() -> {
            try {
                Thread.sleep(BALANCING_TRIGGER_MILLIS);

                String processorName = trackingProcessor.name();
                ClientProcessorsForContextAndName matchingClients = new ClientProcessorsForContextAndName(
                        allClientProcessors, trackingProcessor.context(), processorName
                );

                Set<String> clientNames = StreamSupport.stream(matchingClients.spliterator(), false)
                                                       .map(ClientProcessor::clientId)
                                                       .collect(Collectors.toSet());
                CountDownLatch clientProcessorStatusUpdateLatch = new CountDownLatch(clientNames.size());
                Consumer<EventProcessorStatusUpdated> statusUpdateListener = statusEvent -> {
                    boolean removed = clientNames.remove(statusEvent.eventProcessorStatus().getClientName());
                    if (removed) {
                        clientProcessorStatusUpdateLatch.countDown();
                    }
                };

                updateListeners.add(statusUpdateListener);
                matchingClients.forEach(client -> eventPublisher.publishEvent(
                        new ProcessorStatusRequest(client.clientId(), processorName, false)
                ));
                boolean updated = clientProcessorStatusUpdateLatch.await(10, SECONDS);
                updateListeners.remove(statusUpdateListener);

                if (updated) {
                    String strategyName = loadBalancingService.findById(trackingProcessor)
                                                              .map(RaftProcessorLoadBalancing::strategy)
                                                              .orElse(DEFAULT_STRATEGY_NAME);

                    loadBalancingStrategy.balance(trackingProcessor, strategyName)
                                         .perform();
                }
            } catch (InterruptedException e) {
                logger.warn("Thread interrupted during Load Balancing Operation", e);
                Thread.currentThread().interrupt();
            }
        });
    }
}