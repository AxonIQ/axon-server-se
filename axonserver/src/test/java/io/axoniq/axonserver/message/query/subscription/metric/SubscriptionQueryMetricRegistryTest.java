/*
 *  Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.query.subscription.metric;

import io.axoniq.axonserver.applicationevents.SubscriptionQueryEvents;
import io.axoniq.axonserver.applicationevents.SubscriptionQueryEvents.SubscriptionQueryCanceled;
import io.axoniq.axonserver.grpc.query.QueryRequest;
import io.axoniq.axonserver.grpc.query.QueryUpdate;
import io.axoniq.axonserver.grpc.query.SubscriptionQuery;
import io.axoniq.axonserver.grpc.query.SubscriptionQueryResponse;
import io.axoniq.axonserver.metric.BaseMetricName;
import io.axoniq.axonserver.metric.DefaultMetricCollector;
import io.axoniq.axonserver.metric.MeterFactory;
import io.axoniq.axonserver.metric.MetricCollector;
import io.axoniq.axonserver.test.FakeClock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;

import static org.junit.Assert.assertEquals;


/**
 * @author Marc Gathier
 */
public class SubscriptionQueryMetricRegistryTest {

    private SubscriptionQueryMetricRegistry testSubject;
    private final FakeClock clock = new FakeClock();
    private final MeterRegistry meterRegistry = new SimpleMeterRegistry();

    @Before
    public void setUp() {
        MetricCollector metricCollector = new DefaultMetricCollector();
        testSubject = new SubscriptionQueryMetricRegistry(new MeterFactory(meterRegistry,
                                                                           metricCollector),
                                                          metricCollector::apply,
                                                          clock);
    }

    @Test
    public void getInitial() {
        HubSubscriptionMetrics metric = testSubject.getByComponentAndContext("myComponent", "myContext");
        assertEquals(0, (long) metric.activesCount());
    }

    @Test
    public void getAfterSubscribe() {
        testSubject.on(new SubscriptionQueryEvents.SubscriptionQueryStarted("myContext",
                                                                            query("Subscription-1"),
                                                                            null,
                                                                            null));
        HubSubscriptionMetrics metric = testSubject.getByComponentAndContext("myComponent", "myContext");
        assertEquals(1, (long) metric.activesCount());

        testSubject.on(new SubscriptionQueryEvents.SubscriptionQueryResponseReceived(SubscriptionQueryResponse
                                                                                             .newBuilder()
                                                                                             .setSubscriptionIdentifier(
                                                                                                     "Subscription-1")
                                                                                             .setUpdate(
                                                                                                     QueryUpdate
                                                                                                             .newBuilder()
                                                                                             )
                                                                                             .build(), "handlerId"));
        metric = testSubject.getByComponentAndContext("myComponent", "myContext");
        assertEquals(1, (long) metric.activesCount());
        assertEquals(1, (long) metric.totalCount());
        assertEquals(1, (long) metric.updatesCount());
        metric = testSubject.getByRequestAndContext("query", "myContext");
        assertEquals(1, (long) metric.activesCount());
        assertEquals(1, (long) metric.totalCount());
        assertEquals(1, (long) metric.updatesCount());
        metric = testSubject.get("myContext");
        assertEquals(1, (long) metric.activesCount());
        assertEquals(1, (long) metric.totalCount());
        assertEquals(1, (long) metric.updatesCount());
    }

    @Nonnull
    private SubscriptionQuery query(String subscriptionId) {
        return SubscriptionQuery.newBuilder()
                                .setSubscriptionIdentifier(subscriptionId)
                                .setQueryRequest(QueryRequest
                                                         .newBuilder()
                                                         .setComponentName("myComponent")
                                                         .setQuery("query")
                                ).build();
    }

    @Test
    public void subscriptionCancelledBeforeStartedTest() {
        HubSubscriptionMetrics metrics = testSubject.getByComponentAndContext("myComponent", "context");
        assertEquals(0L, metrics.activesCount().longValue());
        testSubject.on(new SubscriptionQueryCanceled("context", query("2")));
        assertEquals(0L, metrics.activesCount().longValue());
        assertEquals(0L, metrics.updatesCount().longValue());
    }

    @Test
    public void subscriptionCancelledAfterStartedTest() {
        HubSubscriptionMetrics metrics = testSubject.getByComponentAndContext("myComponent", "context");
        testSubject.on(new SubscriptionQueryEvents.SubscriptionQueryStarted("context", query("3"), (u, c) -> {
        }, t -> {
        }));
        assertEquals(1L, metrics.activesCount().longValue());
        clock.timeElapses(10);
        testSubject.on(new SubscriptionQueryCanceled("context", query("3")));
        assertEquals(0L, metrics.activesCount().longValue());
        assertEquals(0L, metrics.updatesCount().longValue());
        HistogramSnapshot snapshot = meterRegistry.timer(BaseMetricName.AXON_SUBSCRIPTION_DURATION.metric(),
                                                         Tags.of(
                                                                 MeterFactory.REQUEST,
                                                                 "query",
                                                                 MeterFactory.CONTEXT,
                                                                 "context",
                                                                 SubscriptionQueryMetricRegistry.TAG_COMPONENT,
                                                                 "myComponent"))
                                                  .takeSnapshot();
        assertEquals(1, snapshot.count());
        assertEquals(10, (int) snapshot.max(TimeUnit.MILLISECONDS));
    }
}