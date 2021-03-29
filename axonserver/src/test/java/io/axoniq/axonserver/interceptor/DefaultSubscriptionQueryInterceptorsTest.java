/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.interceptor;

import io.axoniq.axonserver.plugin.PluginKey;
import io.axoniq.axonserver.plugin.ServiceWithInfo;
import io.axoniq.axonserver.plugin.interceptor.SubscriptionQueryRequestInterceptor;
import io.axoniq.axonserver.plugin.interceptor.SubscriptionQueryResponseInterceptor;
import io.axoniq.axonserver.grpc.query.QueryRequest;
import io.axoniq.axonserver.grpc.query.QueryUpdateComplete;
import io.axoniq.axonserver.grpc.query.SubscriptionQuery;
import io.axoniq.axonserver.grpc.query.SubscriptionQueryRequest;
import io.axoniq.axonserver.grpc.query.SubscriptionQueryResponse;
import io.axoniq.axonserver.metric.DefaultMetricCollector;
import io.axoniq.axonserver.metric.MeterFactory;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.*;

import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class DefaultSubscriptionQueryInterceptorsTest {

    public static final PluginKey PLUGIN_KEY = new PluginKey("sample", "1.0");
    private final TestPluginServiceProvider osgiController = new TestPluginServiceProvider();
    private final PluginContextFilter pluginContextFilter = new PluginContextFilter(osgiController, true);

    private final MeterFactory meterFactory = new MeterFactory(new SimpleMeterRegistry(),
                                                               new DefaultMetricCollector());
    private final DefaultSubscriptionQueryInterceptors testSubject = new DefaultSubscriptionQueryInterceptors(
            pluginContextFilter, meterFactory);

    @Test
    public void queryRequest() {
        osgiController
                .add(new ServiceWithInfo<>((SubscriptionQueryRequestInterceptor) (queryRequest, executionContext) ->
                        SubscriptionQueryRequest.newBuilder(queryRequest)
                                                .setSubscribe(SubscriptionQuery.newBuilder()
                                                                               .setQueryRequest(QueryRequest
                                                                                                        .newBuilder()))
                                                .build(),
                                           PLUGIN_KEY));

        SubscriptionQueryRequest intercepted = testSubject.subscriptionQueryRequest(SubscriptionQueryRequest
                                                                                            .getDefaultInstance(),
                                                                                    new TestExecutionContext(
                                                                                            "default"));
        assertFalse(intercepted.hasSubscribe());

        pluginContextFilter.on(new PluginEnabledEvent("default", PLUGIN_KEY, null, true));
        intercepted = testSubject.subscriptionQueryRequest(SubscriptionQueryRequest.getDefaultInstance(),
                                                           new TestExecutionContext("default"));
        assertTrue(intercepted.hasSubscribe());
    }

    @Test
    public void queryResponse() {
        osgiController
                .add(new ServiceWithInfo<>((SubscriptionQueryResponseInterceptor) (queryResponse, executionContext) ->
                        SubscriptionQueryResponse.newBuilder(queryResponse)
                                                 .setComplete(QueryUpdateComplete.newBuilder()).build(),
                                           PLUGIN_KEY));

        SubscriptionQueryResponse intercepted = testSubject.subscriptionQueryResponse(SubscriptionQueryResponse
                                                                                              .getDefaultInstance(),
                                                                                      new TestExecutionContext(
                                                                                              "default"));
        assertFalse(intercepted.hasComplete());

        pluginContextFilter.on(new PluginEnabledEvent("default", PLUGIN_KEY, null, true));
        intercepted = testSubject.subscriptionQueryResponse(SubscriptionQueryResponse.getDefaultInstance(),
                                                            new TestExecutionContext("default"));
        assertTrue(intercepted.hasComplete());
    }
}