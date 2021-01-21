/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.applicationevents.SubscriptionQueryEvents;
import io.axoniq.axonserver.extensions.ExtensionUnitOfWork;
import io.axoniq.axonserver.grpc.query.SubscriptionQuery;
import io.axoniq.axonserver.grpc.query.SubscriptionQueryRequest;
import io.axoniq.axonserver.grpc.query.SubscriptionQueryResponse;
import io.axoniq.axonserver.interceptor.SubscriptionQueryInterceptors;
import io.axoniq.axonserver.test.FakeStreamObserver;
import org.junit.*;
import org.springframework.context.ApplicationEventPublisher;

import java.util.UUID;

import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class SubscriptionQueryRequestTargetTest {

    private SubscriptionQueryRequestTarget testSubject;
    private final FakeStreamObserver<SubscriptionQueryResponse> responseStreamObserver = new FakeStreamObserver<>();
    private final TestSubscriptionQueryInterceptors interceptors = new TestSubscriptionQueryInterceptors();

    @Before
    public void setUp() {
        ApplicationEventPublisher eventPublisher = event -> {
            if (event instanceof SubscriptionQueryEvents.SubscriptionQueryRequested) {
                SubscriptionQueryEvents.SubscriptionQueryRequested requested = (SubscriptionQueryEvents.SubscriptionQueryRequested) event;
                requested.handler().onSubscriptionQueryResponse(SubscriptionQueryResponse.newBuilder().build());
            }
            if (event instanceof SubscriptionQueryEvents.SubscriptionQueryInitialResultRequested) {
                SubscriptionQueryEvents.SubscriptionQueryInitialResultRequested requested = (SubscriptionQueryEvents.SubscriptionQueryInitialResultRequested) event;
                requested.handler().onSubscriptionQueryResponse(SubscriptionQueryResponse.newBuilder().build());
            }
        };

        testSubject = new SubscriptionQueryRequestTarget("context",
                                                         null,
                                                         responseStreamObserver,
                                                         interceptors,
                                                         eventPublisher);
    }

    @Test
    public void testInterceptorsOnSubscribe() {
        testSubject.consume(SubscriptionQueryRequest.newBuilder().setSubscribe(SubscriptionQuery.getDefaultInstance())
                                                    .build());
        assertEquals(1, interceptors.subscriptionQueryRequestCount);
        assertEquals(1, interceptors.subscriptionQueryResponseCount);
    }

    private static class TestSubscriptionQueryInterceptors implements SubscriptionQueryInterceptors {

        int subscriptionQueryRequestCount;
        int subscriptionQueryResponseCount;
        UUID lastUUID;

        @Override
        public SubscriptionQueryRequest subscriptionQueryRequest(SubscriptionQueryRequest subscriptionQueryRequest,
                                                                 ExtensionUnitOfWork extensionContext) {
            subscriptionQueryRequestCount++;
            extensionContext.addDetails("RequestId", UUID.randomUUID());
            return subscriptionQueryRequest;
        }

        @Override
        public SubscriptionQueryResponse subscriptionQueryResponse(SubscriptionQueryResponse subscriptionQueryResponse,
                                                                   ExtensionUnitOfWork extensionContext) {
            subscriptionQueryResponseCount++;
            lastUUID = (UUID) extensionContext.getDetails("RequestId");
            return subscriptionQueryResponse;
        }
    }
}