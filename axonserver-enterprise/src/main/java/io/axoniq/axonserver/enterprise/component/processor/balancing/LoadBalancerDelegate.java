package io.axoniq.axonserver.enterprise.component.processor.balancing;

import io.axoniq.axonserver.component.processor.EventProcessorIdentifier;
import io.axoniq.axonserver.component.processor.EventProcessorStatusRefresh;
import io.axoniq.axonserver.component.processor.balancing.TrackingEventProcessor;
import io.axoniq.axonserver.component.processor.balancing.strategy.ProcessorLoadBalanceStrategy;
import io.axoniq.axonserver.enterprise.component.processor.balancing.jpa.ReplicationGroupProcessorLoadBalancing;
import io.axoniq.axonserver.enterprise.component.processor.balancing.stategy.ReplicationGroupProcessorLoadBalancingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Implementation which delegates the balancing of a single {@link TrackingEventProcessor}. To that end, it will prior
 * to balancing request the most recent status of the given TrackingEventProcessor on all the clients which are part of
 * the processor's context and which contain the given processor. Once all the statuses have been updated, it delegates
 * the balancing operation towards the {@link ProcessorLoadBalanceStrategy} using the strategy as defined in the
 * {@link ReplicationGroupProcessorLoadBalancingService}.
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

    private final ReplicationGroupProcessorLoadBalancingService loadBalancingService;
    private final ProcessorLoadBalanceStrategy loadBalancingStrategy;

    private final Map<TrackingEventProcessor, ScheduledExecutorService> executors = new HashMap<>();
    private final BiFunction<String, EventProcessorIdentifier, CompletableFuture<Void>> refreshEventProcessorStatus;

    /**
     * Build a delegate Load Balancer, which ensure the most recent status of {@link TrackingEventProcessor} to balance
     * is known prior to issuing the act of load balancing.
     *
     * @param refreshEventProcessorStatus a {@link BiFunction} used to trigger the refresh of all clients' event
     *                                    processors status in order to perform the balancing based on updated data
     * @param loadBalancingService        the {@link ReplicationGroupProcessorLoadBalancingService} used to deduce the
     *                                    load
     *                                    balancing strategy to use
     * @param loadBalancingStrategy       the {@link ProcessorLoadBalanceStrategy} which actually performs the act of
     *                                    balancing the load for the given {@link TrackingEventProcessor}
     */
    @Autowired
    public LoadBalancerDelegate(EventProcessorStatusRefresh refreshEventProcessorStatus,
                                ReplicationGroupProcessorLoadBalancingService loadBalancingService,
                                ProcessorLoadBalanceStrategy loadBalancingStrategy) {
        this.refreshEventProcessorStatus = refreshEventProcessorStatus::run;
        this.loadBalancingStrategy = loadBalancingStrategy;
        this.loadBalancingService = loadBalancingService;
    }

    /**
     * Build a delegate Load Balancer, which ensure the most recent status of {@link TrackingEventProcessor} to balance
     * is known prior to issuing the act of load balancing.
     *
     * @param refreshEventProcessorStatus a {@link Function} used to trigger the refresh of all clients' event
     *                                    processors status for a given {@link EventProcessorIdentifier} in order to
     *                                    perform the balancing based on updated data
     * @param loadBalancingService        the {@link ReplicationGroupProcessorLoadBalancingService} used to deduce the load
     *                                    balancing strategy to use
     * @param loadBalancingStrategy       the {@link ProcessorLoadBalanceStrategy} which actually performs the act of
     *                                    balancing the load for the given {@link TrackingEventProcessor}
     */
    public LoadBalancerDelegate(
            BiFunction<String, EventProcessorIdentifier, CompletableFuture<Void>> refreshEventProcessorStatus,
            ReplicationGroupProcessorLoadBalancingService loadBalancingService,
            ProcessorLoadBalanceStrategy loadBalancingStrategy) {
        this.refreshEventProcessorStatus = refreshEventProcessorStatus;
        this.loadBalancingStrategy = loadBalancingStrategy;
        this.loadBalancingService = loadBalancingService;
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
            refreshEventProcessorStatus
                    .apply(trackingProcessor.context(), new EventProcessorIdentifier(trackingProcessor))
                    .thenRun(() -> performBalancing(trackingProcessor))
                    .exceptionally(throwable -> {
                        logger.warn("Load Balancing Operation failed", throwable);
                        return null;
                    });
        }, BALANCING_TRIGGER_MILLIS, TimeUnit.MILLISECONDS);
    }

    private void performBalancing(TrackingEventProcessor trackingProcessor) {
        String strategyName = loadBalancingService.findById(trackingProcessor)
                                                  .map(ReplicationGroupProcessorLoadBalancing::strategy)
                                                  .orElse(DEFAULT_STRATEGY_NAME);

        loadBalancingStrategy.balance(trackingProcessor, strategyName)
                             .perform();
    }
}