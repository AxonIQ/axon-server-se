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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Implementation which delegates the balancing of a single {@link TrackingEventProcessor}. To that end, it will prior
 * to balancing request the most recent status of the given TrackingEventProcessor on all the clients which are part of
 * the processor's context and which contain the given processor. Once all the statuses have been updated, it delegates
 * the balancing operation towards the {@link ProcessorLoadBalanceStrategy} using the strategy as defined in the
 * {@link RaftProcessorLoadBalancingService}.
 *
 * @author Sara Pellegrini
 * @since 4.0
 */
@Component
public class LoadBalancerDelegate {

    private static final Logger logger = LoggerFactory.getLogger(LoadBalancerDelegate.class);

    private static final int CORE_POOL_SIZE = 1;
    private static final int BALANCING_TRIGGER_MILLIS = 15000;
    private static final String DEFAULT_STRATEGY_NAME = "default";

    private final ClientProcessors allClientProcessors;
    private final ApplicationEventPublisher eventPublisher;
    private final RaftProcessorLoadBalancingService loadBalancingService;
    private final ProcessorLoadBalanceStrategy loadBalancingStrategy;

    private final Map<TrackingEventProcessor, ScheduledExecutorService> executors = new HashMap<>();
    private final List<Consumer<EventProcessorStatusUpdated>> updateListeners = new CopyOnWriteArrayList<>();

    /**
     * Build a delegate Load Balancer, which ensure the most recent status of {@link TrackingEventProcessor} to balance
     * is known prior to issuing the act of load balancing.
     *
     * @param allClientProcessors   a {@link ClientProcessors} containing all known {@link ClientProcessor}s in the
     *                              cluster
     * @param eventPublisher        {@link ApplicationEventPublisher} implementation capable of publishing Spring
     *                              application events
     * @param loadBalancingService  the {@link RaftProcessorLoadBalancingService} used to deduce the load balancing
     *                              strategy to use
     * @param loadBalancingStrategy the {@link ProcessorLoadBalanceStrategy} which actually performs the act of
     *                              balancing the load for the given {@link TrackingEventProcessor}
     */
    public LoadBalancerDelegate(ClientProcessors allClientProcessors,
                                ApplicationEventPublisher eventPublisher,
                                RaftProcessorLoadBalancingService loadBalancingService,
                                ProcessorLoadBalanceStrategy loadBalancingStrategy) {
        this.allClientProcessors = allClientProcessors;
        this.eventPublisher = eventPublisher;
        this.loadBalancingStrategy = loadBalancingStrategy;
        this.loadBalancingService = loadBalancingService;
    }

    /**
     * Event handler towards the {@link EventProcessorStatusUpdated}, used to verify if all the status updates have been
     * received.
     *
     * @param event the {@link EventProcessorStatusUpdated} signaling the update of Event Processor Status
     */
    @EventListener
    public void on(EventProcessorStatusUpdated event) {
        updateListeners.forEach(listener -> listener.accept(event));
    }

    /**
     * Delegate balance operation, which ensure the most recent status of the given {@code trackingProcessor} is known
     * prior to performing the actual load balancing operation. Sending and waiting for the status updates to come in
     * is performed in a separate thread created through a dedicated {@link ExecutorService} per incoming
     * {@code trackingProcessor}.
     *
     * @param trackingProcessor the {@link TrackingEventProcessor} to balance the load for
     */
    public void balance(TrackingEventProcessor trackingProcessor) {
        ScheduledExecutorService scheduledExecutor =
                executors.computeIfAbsent(trackingProcessor, tep -> new ScheduledThreadPoolExecutor(CORE_POOL_SIZE));

        scheduledExecutor.schedule(() -> {
            try {
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
        }, BALANCING_TRIGGER_MILLIS, TimeUnit.MILLISECONDS);
    }
}