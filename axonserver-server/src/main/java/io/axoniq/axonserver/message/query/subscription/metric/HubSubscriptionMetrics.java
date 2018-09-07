package io.axoniq.axonserver.message.query.subscription.metric;

import com.codahale.metrics.Counter;
import io.axoniq.axonserver.message.query.subscription.SubscriptionMetrics;
import io.axoniq.axonserver.metric.ClusterMetric;
import io.axoniq.axonserver.metric.CompositeMetric;
import io.axoniq.axonserver.metric.CounterMetric;
import io.axoniq.axonserver.serializer.Media;

import java.util.function.Function;

/**
 * Created by Sara Pellegrini on 19/06/2018.
 * sara.pellegrini@gmail.com
 */
public class HubSubscriptionMetrics implements SubscriptionMetrics {

    private final ClusterMetric totalSubscriptions;
    private final ClusterMetric activeSubscriptions;
    private final ClusterMetric updates;

    public HubSubscriptionMetrics(String active, String total, String updates,
                                  Function<String, Counter> localRegistry,
                                  Function<String, ClusterMetric> clusterRegistry) {
        this(
                new CompositeMetric(new CounterMetric(localRegistry.apply(total)), clusterRegistry.apply(total)),
                new CompositeMetric(new CounterMetric(localRegistry.apply(active)), clusterRegistry.apply(active)),
                new CompositeMetric(new CounterMetric(localRegistry.apply(updates)), clusterRegistry.apply(updates))
        );
    }

    public HubSubscriptionMetrics(ClusterMetric totalSubscriptions,
                                  ClusterMetric activeSubscriptions, ClusterMetric updates) {
        this.totalSubscriptions = totalSubscriptions;
        this.activeSubscriptions = activeSubscriptions;
        this.updates = updates;
    }


    @Override
    public Long totalCount() {
        return totalSubscriptions.size();
    }

    @Override
    public Long activesCount() {
        return activeSubscriptions.size();
    }

    @Override
    public Long updatesCount() {
        return updates.size();
    }


    @Override
    public void printOn(Media media) {
        media.with("totalSubscriptions", totalCount());
        media.with("activeSubscriptions", activesCount());
        media.with("updates", updatesCount());
    }
}
