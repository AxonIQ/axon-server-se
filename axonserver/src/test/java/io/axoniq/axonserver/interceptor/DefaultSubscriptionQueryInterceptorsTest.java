/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.interceptor;

import io.axoniq.axonserver.extensions.ExtensionKey;
import io.axoniq.axonserver.extensions.ServiceWithInfo;
import io.axoniq.axonserver.extensions.interceptor.SubscriptionQueryRequestInterceptor;
import io.axoniq.axonserver.extensions.interceptor.SubscriptionQueryResponseInterceptor;
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

    public static final ExtensionKey EXTENSION_KEY = new ExtensionKey("sample", "1.0");
    private final TestExtensionServiceProvider osgiController = new TestExtensionServiceProvider();
    private final ExtensionContextFilter extensionContextFilter = new ExtensionContextFilter(osgiController, true);

    private final MeterFactory meterFactory = new MeterFactory(new SimpleMeterRegistry(),
                                                               new DefaultMetricCollector());
    private final DefaultSubscriptionQueryInterceptors testSubject = new DefaultSubscriptionQueryInterceptors(
            extensionContextFilter, meterFactory);

    @Test
    public void queryRequest() {
        osgiController
                .add(new ServiceWithInfo<>((SubscriptionQueryRequestInterceptor) (queryRequest, extensionContext) ->
                        SubscriptionQueryRequest.newBuilder(queryRequest)
                                                .setSubscribe(SubscriptionQuery.newBuilder()
                                                                               .setQueryRequest(QueryRequest
                                                                                                        .newBuilder()))
                                                .build(),
                                           EXTENSION_KEY));

        SubscriptionQueryRequest intercepted = testSubject.subscriptionQueryRequest(SubscriptionQueryRequest
                                                                                            .getDefaultInstance(),
                                                                                    new TestExtensionUnitOfWork(
                                                                                            "default"));
        assertFalse(intercepted.hasSubscribe());

        extensionContextFilter.on(new ExtensionEnabledEvent("default", EXTENSION_KEY, null, true));
        intercepted = testSubject.subscriptionQueryRequest(SubscriptionQueryRequest.getDefaultInstance(),
                                                           new TestExtensionUnitOfWork("default"));
        assertTrue(intercepted.hasSubscribe());
    }

    @Test
    public void queryResponse() {
        osgiController
                .add(new ServiceWithInfo<>((SubscriptionQueryResponseInterceptor) (queryResponse, extensionContext) ->
                        SubscriptionQueryResponse.newBuilder(queryResponse)
                                                 .setComplete(QueryUpdateComplete.newBuilder()).build(),
                                           EXTENSION_KEY));

        SubscriptionQueryResponse intercepted = testSubject.subscriptionQueryResponse(SubscriptionQueryResponse
                                                                                              .getDefaultInstance(),
                                                                                      new TestExtensionUnitOfWork(
                                                                                              "default"));
        assertFalse(intercepted.hasComplete());

        extensionContextFilter.on(new ExtensionEnabledEvent("default", EXTENSION_KEY, null, true));
        intercepted = testSubject.subscriptionQueryResponse(SubscriptionQueryResponse.getDefaultInstance(),
                                                            new TestExtensionUnitOfWork("default"));
        assertTrue(intercepted.hasComplete());
    }
}