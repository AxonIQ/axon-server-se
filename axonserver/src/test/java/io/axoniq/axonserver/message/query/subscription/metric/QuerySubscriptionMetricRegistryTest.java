package io.axoniq.axonserver.message.query.subscription.metric;

import io.axoniq.axonserver.applicationevents.SubscriptionQueryEvents;
import io.axoniq.axonserver.applicationevents.SubscriptionQueryEvents.SubscriptionQueryStarted;
import io.axoniq.axonserver.grpc.query.QueryRequest;
import io.axoniq.axonserver.grpc.query.SubscriptionQuery;
import io.axoniq.axonserver.metric.DefaultMetricCollector;
import io.axoniq.axonserver.metric.MeterFactory;
import io.axoniq.axonserver.metric.MetricCollector;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import junit.framework.TestCase;
import org.junit.*;

import javax.annotation.Nonnull;

/**
 * Unit tests for {@link QuerySubscriptionMetricRegistry}.
 *
 * @author Sara Pellegrini
 */
public class QuerySubscriptionMetricRegistryTest extends TestCase {

    private final String component = "myComponent";
    private final String context = "myContext";
    private final String query = "mQuery";
    private QuerySubscriptionMetricRegistry testSubject;

    @Before
    public void setUp() {
        MetricCollector metricCollector = new DefaultMetricCollector();
        testSubject = new QuerySubscriptionMetricRegistry(new MeterFactory(new SimpleMeterRegistry(),
                                                                           metricCollector), metricCollector);
    }

    public void testTestOnSubscriptionStarted() {
        HubSubscriptionMetrics metrics = testSubject.get(component, query, context);
        assertEquals(0L, metrics.activesCount().longValue());
        testSubject.on(new SubscriptionQueryStarted(context, query("1"), u -> {
        }, t -> {
        }));
        assertEquals(1L, metrics.activesCount().longValue());
    }

    public void testOnSubscriptionCancelledAfterStarted() {
        HubSubscriptionMetrics metrics = testSubject.get(component, query, context);
        assertEquals(0L, metrics.activesCount().longValue());
        testSubject.on(new SubscriptionQueryStarted(context, query("2"), u -> {
        }, t -> {
        }));
        assertEquals(1L, metrics.activesCount().longValue());
        testSubject.on(new SubscriptionQueryEvents.SubscriptionQueryCanceled(context, query("2")));
        assertEquals(0L, metrics.activesCount().longValue());
    }

    public void testTestOnSubscriptionCancelledBeforeStarted() {
        HubSubscriptionMetrics metrics = testSubject.get(component, query, context);
        assertEquals(0L, metrics.activesCount().longValue());
        testSubject.on(new SubscriptionQueryEvents.SubscriptionQueryCanceled(context, query("2")));
        assertEquals(0L, metrics.activesCount().longValue());
    }

    @Nonnull
    private SubscriptionQuery query(String subscriptionId) {
        return SubscriptionQuery.newBuilder()
                                .setSubscriptionIdentifier(subscriptionId)
                                .setQueryRequest(QueryRequest
                                                         .newBuilder()
                                                         .setQuery(query)
                                                         .setComponentName(component)
                                ).build();
    }
}