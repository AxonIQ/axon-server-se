package io.axoniq.axonserver.message.query.subscription.metric;

import io.axoniq.axonserver.applicationevents.SubscriptionQueryEvents;
import io.axoniq.axonserver.message.query.subscription.SubscriptionMetrics;
import io.axoniq.axonserver.metric.ClusterMetric;
import io.axoniq.axonserver.metric.CounterMetric;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.axoniq.axonserver.grpc.query.SubscriptionQueryResponse.ResponseCase.UPDATE;

/**
 * Created by Sara Pellegrini on 20/06/2018.
 * sara.pellegrini@gmail.com
 */
@Component
public class GlobalSubscriptionMetricRegistry implements Supplier<SubscriptionMetrics> {

    private final Function<String, ClusterMetric> clusterMetricRegistry;
    private final Set<String> subscriptions = new CopyOnWriteArraySet<>();
    private final Counter total;

    private final AtomicInteger active = new AtomicInteger(0);
    private final Counter updates;

    public GlobalSubscriptionMetricRegistry(MeterRegistry localMetricRegistry,
                                                 Function<String, ClusterMetric> clusterMetricRegistry) {
        this.clusterMetricRegistry = clusterMetricRegistry;

        this.updates = localMetricRegistry.counter(name(GlobalSubscriptionMetricRegistry.class.getSimpleName(), "updates"));
        this.total = localMetricRegistry.counter(name(GlobalSubscriptionMetricRegistry.class.getSimpleName(), "total"));

        Gauge.builder(name(GlobalSubscriptionMetricRegistry.class.getSimpleName(), "active"), active, AtomicInteger::get).register(localMetricRegistry);
    }

    @Override
    public HubSubscriptionMetrics get() {
        return new HubSubscriptionMetrics(
                new CounterMetric(name(GlobalSubscriptionMetricRegistry.class.getSimpleName(), "active"), ()-> (long)active.get()),
                new CounterMetric(total.getId().getName(), () -> (long)total.count()),
                new CounterMetric(updates.getId().getName(), () -> (long)updates.count()),
                clusterMetricRegistry);
    }

    private String name(String name, String total) {
        return String.format("axon.%s.%s", name, total);
    }


    @EventListener
    public void on(SubscriptionQueryEvents.SubscriptionQueryRequested event){
        active.incrementAndGet();
        total.increment();
        subscriptions.add(event.subscriptionId());
    }

    @EventListener
    public void on(SubscriptionQueryEvents.SubscriptionQueryCanceled event){
        active.decrementAndGet();
        subscriptions.remove(event.subscriptionId());
    }

    @EventListener
    public void on(SubscriptionQueryEvents.SubscriptionQueryResponseReceived event){
        if (subscriptions.contains(event.subscriptionId()) && event.response().getResponseCase().equals(UPDATE)){
            updates.increment();
        }
    }
}
