package io.axoniq.axonhub.message.query.subscription.metric;

import com.codahale.metrics.MetricRegistry;
import io.axoniq.axonhub.SubscriptionQueryEvents;
import io.axoniq.axonhub.metric.ClusterMetric;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import static com.codahale.metrics.MetricRegistry.name;
import static io.axoniq.axonhub.SubscriptionQueryResponse.ResponseCase.UPDATE;

/**
 * Created by Sara Pellegrini on 20/06/2018.
 * sara.pellegrini@gmail.com
 */
@Component
public class ApplicationSubscriptionMetricRegistry {

    private final MetricRegistry localMetricRegistry;
    private final Function<String, ClusterMetric> clusterMetricRegistry;
    private final Map<String, String> componentNames = new ConcurrentHashMap<>();
    private final Map<String, String> contexts = new ConcurrentHashMap<>();

    public ApplicationSubscriptionMetricRegistry(MetricRegistry localMetricRegistry,
                                                 Function<String, ClusterMetric> clusterMetricRegistry) {
        this.localMetricRegistry = localMetricRegistry;
        this.clusterMetricRegistry = clusterMetricRegistry;
    }

    public HubSubscriptionMetrics get(String componentName, String context) {
        String active = activeSubscriptionsMetric(componentName, context);
        String total = totalSubscriptionsMetric(componentName, context);
        String updates = updatesMetric(componentName, context);
        return new HubSubscriptionMetrics(active, total, updates, localMetricRegistry::counter, clusterMetricRegistry);
    }


    @EventListener
    public void on(SubscriptionQueryEvents.SubscriptionQueryRequested event){
        String componentName = event.subscription().getQueryRequest().getComponentName();
        String context = event.context();
        localMetricRegistry.counter(activeSubscriptionsMetric(componentName,context)).inc();
        localMetricRegistry.counter(totalSubscriptionsMetric(componentName,context)).inc();
        componentNames.putIfAbsent(event.subscriptionId(), componentName);
        contexts.putIfAbsent(event.subscriptionId(), context);
    }

    @EventListener
    public void on(SubscriptionQueryEvents.SubscriptionQueryCanceled event){
        String componentName = event.unsubscribe().getQueryRequest().getComponentName();
        String context = contexts.remove(event.subscriptionId());
        localMetricRegistry.counter(activeSubscriptionsMetric(componentName, context)).dec();
        componentNames.remove(event.subscriptionId());
    }

    @EventListener
    public void on(SubscriptionQueryEvents.SubscriptionQueryResponseReceived event){
        if (componentNames.containsKey(event.subscriptionId()) && event.response().getResponseCase().equals(UPDATE)){
            String component = componentNames.get(event.subscriptionId());
            String context = contexts.get(event.subscriptionId());
            localMetricRegistry.counter(updatesMetric(component, context)).inc();
        }
    }

    private String activeSubscriptionsMetric(String component, String context){
        return name(ApplicationSubscriptionMetricRegistry.class.getName(), component, context, "active");
    }

    private String totalSubscriptionsMetric(String component, String context){
        return name(ApplicationSubscriptionMetricRegistry.class.getName(), component, context, "total");
    }

    private String updatesMetric(String component, String context){
        return name(ApplicationSubscriptionMetricRegistry.class.getName(),component, context, "updates");
    }
}
