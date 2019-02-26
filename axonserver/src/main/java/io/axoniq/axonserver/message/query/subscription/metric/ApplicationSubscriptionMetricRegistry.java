package io.axoniq.axonserver.message.query.subscription.metric;

import io.axoniq.axonserver.applicationevents.SubscriptionQueryEvents;
import io.axoniq.axonserver.metric.ClusterMetric;
import io.axoniq.axonserver.metric.CounterMetric;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static io.axoniq.axonserver.grpc.query.SubscriptionQueryResponse.ResponseCase.UPDATE;

/**
 * Created by Sara Pellegrini on 20/06/2018.
 * sara.pellegrini@gmail.com
 */
@Component
public class ApplicationSubscriptionMetricRegistry {

    private final MeterRegistry localMetricRegistry;
    private final Function<String, ClusterMetric> clusterMetricRegistry;
    private final Map<String, String> componentNames = new ConcurrentHashMap<>();
    private final Map<String, String> contexts = new ConcurrentHashMap<>();
    private final Map<String, Counter> totalSubscriptionsMap = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> activeSubscriptionsMap = new ConcurrentHashMap<>();
    private final Map<String, Counter> updatesMap = new ConcurrentHashMap<>();

    public ApplicationSubscriptionMetricRegistry(MeterRegistry localMetricRegistry,
                                                 Function<String, ClusterMetric> clusterMetricRegistry) {
        this.localMetricRegistry = localMetricRegistry;
        this.clusterMetricRegistry = clusterMetricRegistry;
    }

    public HubSubscriptionMetrics get(String componentName, String context) {
        AtomicInteger active = activeSubscriptionsMetric(componentName, context);
        Counter total = totalSubscriptionsMetric(componentName, context);
        Counter updates = updatesMetric(componentName, context);
        return new HubSubscriptionMetrics(new CounterMetric(activeSubscriptionsMetricName(componentName, context), ()-> (long)active.get()),
                                          new CounterMetric(total.getId().getName(), () -> (long)total.count()),
                                          new CounterMetric(updates.getId().getName(), () -> (long)updates.count()), clusterMetricRegistry);
    }



    @EventListener
    public void on(SubscriptionQueryEvents.SubscriptionQueryRequested event){
        String componentName = event.subscription().getQueryRequest().getComponentName();
        String context = event.context();
        activeSubscriptionsMetric(componentName,context).incrementAndGet();
        totalSubscriptionsMetric(componentName,context).increment();
        componentNames.putIfAbsent(event.subscriptionId(), componentName);
        contexts.putIfAbsent(event.subscriptionId(), context);
    }

    @EventListener
    public void on(SubscriptionQueryEvents.SubscriptionQueryCanceled event){
        String componentName = event.unsubscribe().getQueryRequest().getComponentName();
        String context = contexts.remove(event.subscriptionId());
        activeSubscriptionsMetric(componentName, context).decrementAndGet();
        componentNames.remove(event.subscriptionId());
    }

    @EventListener
    public void on(SubscriptionQueryEvents.SubscriptionQueryResponseReceived event){
        if (componentNames.containsKey(event.subscriptionId()) && event.response().getResponseCase().equals(UPDATE)){
            String component = componentNames.get(event.subscriptionId());
            String context = contexts.get(event.subscriptionId());
            updatesMetric(component, context).increment();
        }
    }

    private AtomicInteger activeSubscriptionsMetric(String component, String context){
        return activeSubscriptionsMap.computeIfAbsent(activeSubscriptionsMetricName(component, context), name -> {
            AtomicInteger atomicInteger = new AtomicInteger(0);
            Gauge.builder(name, atomicInteger, AtomicInteger::get).register(localMetricRegistry);
            return atomicInteger;
        });
    }

    private String activeSubscriptionsMetricName(String component, String context) {
        return name(ApplicationSubscriptionMetricRegistry.class.getSimpleName(), component, context, "active");
    }

    private String name(String name, String component, String context, String active) {
        return String.format("axon.%s.%s.%s.%s", name, component, context, active);
    }

    private Counter totalSubscriptionsMetric(String component, String context){
        return totalSubscriptionsMap.computeIfAbsent(name(ApplicationSubscriptionMetricRegistry.class.getSimpleName(), component, context, "total"),
                                                     localMetricRegistry::counter);
    }

    private Counter updatesMetric(String component, String context){
        return updatesMap.computeIfAbsent(name(ApplicationSubscriptionMetricRegistry.class.getSimpleName(),component, context, "updates"), localMetricRegistry::counter);
    }
}
