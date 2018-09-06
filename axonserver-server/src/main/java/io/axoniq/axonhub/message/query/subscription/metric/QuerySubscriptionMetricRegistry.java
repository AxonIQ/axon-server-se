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
public class QuerySubscriptionMetricRegistry  {
    private final MetricRegistry localRegistry;
    private final Function<String, ClusterMetric> clusterRegistry;
    private final Map<String, String> queries = new ConcurrentHashMap<>();
    private final Map<String, String> contexts = new ConcurrentHashMap<>();

    public QuerySubscriptionMetricRegistry(MetricRegistry localRegistry,
                                                 Function<String, ClusterMetric> clusterRegistry) {
        this.localRegistry = localRegistry;
        this.clusterRegistry = clusterRegistry;
    }

    public HubSubscriptionMetrics get(String component, String query, String context) {
        String active = activeSubscriptionsMetric(query, context);
        String total = totalSubscriptionsMetric(query, context);
        String updates = updatesMetric(component,query, context);
        return new HubSubscriptionMetrics(active, total, updates, localRegistry::counter, clusterRegistry);
    }


    @EventListener
    public void on(SubscriptionQueryEvents.SubscriptionQueryRequested event){
        String queryName = event.subscription().getQueryRequest().getQuery();
        String context = event.context();
        localRegistry.counter(activeSubscriptionsMetric(queryName, context)).inc();
        localRegistry.counter(totalSubscriptionsMetric(queryName, context)).inc();
        queries.put(event.subscriptionId(), queryName);
        contexts.put(event.subscriptionId(), context);
    }

    @EventListener
    public void on(SubscriptionQueryEvents.SubscriptionQueryCanceled event){
        String queryName = event.unsubscribe().getQueryRequest().getQuery();
        String context = contexts.remove(event.subscriptionId());
        localRegistry.counter(activeSubscriptionsMetric(queryName, context)).dec();
        queries.remove(event.subscriptionId(), queryName);
    }

    @EventListener
    public void on(SubscriptionQueryEvents.SubscriptionQueryResponseReceived event){
        if (queries.containsKey(event.subscriptionId()) && event.response().getResponseCase().equals(UPDATE)){
            String componentName = event.response().getUpdate().getComponentName();
            String query = queries.get(event.subscriptionId());
            String context = contexts.get(event.subscriptionId());
            localRegistry.counter(updatesMetric(componentName, query, context)).inc();
        }
    }

    private String activeSubscriptionsMetric(String query, String context){
        return name(QuerySubscriptionMetricRegistry.class.getName(), query, context, "active");
    }

    private String totalSubscriptionsMetric(String query, String context){
        return name(QuerySubscriptionMetricRegistry.class.getName(), query, context, "total");
    }

    private String updatesMetric(String component, String query, String context){
        return name(QuerySubscriptionMetricRegistry.class.getName(),component, query, context, "updates");
    }

}
