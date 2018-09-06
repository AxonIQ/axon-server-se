package io.axoniq.axonhub.message.query.subscription.metric;

import com.codahale.metrics.MetricRegistry;
import io.axoniq.axonhub.SubscriptionQueryEvents;
import io.axoniq.axonhub.message.query.subscription.SubscriptionMetrics;
import io.axoniq.axonhub.metric.ClusterMetric;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.codahale.metrics.MetricRegistry.name;
import static io.axoniq.axonhub.SubscriptionQueryResponse.ResponseCase.UPDATE;

/**
 * Created by Sara Pellegrini on 20/06/2018.
 * sara.pellegrini@gmail.com
 */
@Component
public class GlobalSubscriptionMetricRegistry implements Supplier<SubscriptionMetrics> {

    private final MetricRegistry localMetricRegistry;
    private final Function<String, ClusterMetric> clusterMetricRegistry;
    private final Set<String> subscriptions = new CopyOnWriteArraySet<>();
    private final String total = name(GlobalSubscriptionMetricRegistry.class.getName(), "total");
    private final String active = name(GlobalSubscriptionMetricRegistry.class.getName(), "active");
    private final String updates = name(GlobalSubscriptionMetricRegistry.class.getName(), "updates");

    public GlobalSubscriptionMetricRegistry(MetricRegistry localMetricRegistry,
                                                 Function<String, ClusterMetric> clusterMetricRegistry) {
        this.localMetricRegistry = localMetricRegistry;
        this.clusterMetricRegistry = clusterMetricRegistry;
    }

    @Override
    public HubSubscriptionMetrics get() {
        return new HubSubscriptionMetrics(active, total, updates, localMetricRegistry::counter, clusterMetricRegistry);
    }


    @EventListener
    public void on(SubscriptionQueryEvents.SubscriptionQueryRequested event){
        localMetricRegistry.counter(active).inc();
        localMetricRegistry.counter(total).inc();
        subscriptions.add(event.subscriptionId());
    }

    @EventListener
    public void on(SubscriptionQueryEvents.SubscriptionQueryCanceled event){
        localMetricRegistry.counter(active).dec();
        subscriptions.remove(event.subscriptionId());
    }

    @EventListener
    public void on(SubscriptionQueryEvents.SubscriptionQueryResponseReceived event){
        if (subscriptions.contains(event.subscriptionId()) && event.response().getResponseCase().equals(UPDATE)){
            localMetricRegistry.counter(updates).inc();
        }
    }
}
