package io.axoniq.axonserver.message.query.subscription.metric;

import io.axoniq.axonserver.applicationevents.SubscriptionQueryEvents;
import io.axoniq.axonserver.grpc.query.QueryRequest;
import io.axoniq.axonserver.grpc.query.QueryResponse;
import io.axoniq.axonserver.grpc.query.QueryUpdate;
import io.axoniq.axonserver.grpc.query.SubscriptionQuery;
import io.axoniq.axonserver.grpc.query.SubscriptionQueryResponse;
import io.axoniq.axonserver.metric.ClusterMetric;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.*;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;


/**
 * Author: marc
 */
public class ApplicationSubscriptionMetricRegistryTest {

    private ApplicationSubscriptionMetricRegistry testSubject;
    private Map<String, ClusterMetric> clusterMetricMap = new HashMap<>();

    @Before
    public void setUp() {
        testSubject = new ApplicationSubscriptionMetricRegistry(new SimpleMeterRegistry(), metric -> clusterMetricMap.computeIfAbsent(metric, m -> new ClusterMetric(){
            @Override
            public long size() {
                return 0;
            }

            @Override
            public long min() {
                return 0;
            }

            @Override
            public long max() {
                return 0;
            }

            @Override
            public double mean() {
                return 0;
            }
        }));
    }

    @Test
    public void getInitial() {
        HubSubscriptionMetrics metric = testSubject.get("myComponent", "myContext");
        assertEquals(0, (long) metric.activesCount());
    }

    @Test
    public void getAfterSubscribe() {
        testSubject.on(new SubscriptionQueryEvents.SubscriptionQueryRequested("myContext", SubscriptionQuery.newBuilder()
                                                                                                            .setSubscriptionIdentifier("Subscription-1")
                                                                                                            .setQueryRequest(
                                                                                                                    QueryRequest.newBuilder()
                                                                                                                            .setComponentName("myComponent")
                                                                                                                            )
                                                                                                            .build(), null, null));
        HubSubscriptionMetrics metric = testSubject.get("myComponent", "myContext");
        assertEquals(1, (long) metric.activesCount());

        testSubject.on(new SubscriptionQueryEvents.SubscriptionQueryResponseReceived(SubscriptionQueryResponse.newBuilder()
                                                                                                              .setSubscriptionIdentifier("Subscription-1")
                                                                                                              .setUpdate(
                                                                                                                      QueryUpdate.newBuilder()
                                                                                                              )
                                                                                                              .build()));
        metric = testSubject.get("myComponent", "myContext");
        assertEquals(1, (long) metric.activesCount());
        assertEquals(1, (long) metric.updatesCount());
    }
}