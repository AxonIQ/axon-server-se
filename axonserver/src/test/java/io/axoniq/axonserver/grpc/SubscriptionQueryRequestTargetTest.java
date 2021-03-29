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
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.query.SubscriptionQuery;
import io.axoniq.axonserver.grpc.query.SubscriptionQueryRequest;
import io.axoniq.axonserver.grpc.query.SubscriptionQueryResponse;
import io.axoniq.axonserver.interceptor.SubscriptionQueryInterceptors;
import io.axoniq.axonserver.plugin.ExecutionContext;
import io.axoniq.axonserver.test.FakeStreamObserver;
import io.grpc.StatusRuntimeException;
import org.junit.*;
import org.springframework.context.ApplicationEventPublisher;

import java.util.LinkedList;
import java.util.List;
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

    @Test
    public void interceptorOnSubscribeRejects() {
        interceptors.rejectRequest = true;
        testSubject.consume(SubscriptionQueryRequest.newBuilder().setSubscribe(SubscriptionQuery.getDefaultInstance())
                                                    .build());
        assertEquals(1, responseStreamObserver.errors().size());
        StatusRuntimeException exception = (StatusRuntimeException) responseStreamObserver.errors().get(0);
        assertEquals(ErrorCode.SUBSCRIPTION_QUERY_REJECTED_BY_INTERCEPTOR.getGrpcCode().getCode(),
                     exception.getStatus().getCode());
        assertEquals(ErrorCode.SUBSCRIPTION_QUERY_REJECTED_BY_INTERCEPTOR.getCode(),
                     exception.getTrailers().get(GrpcMetadataKeys.ERROR_CODE_KEY));
    }

    @Test
    public void interceptorOnSubscribeFails() {
        interceptors.failRequest = true;
        testSubject.consume(SubscriptionQueryRequest.newBuilder().setSubscribe(SubscriptionQuery.getDefaultInstance())
                                                    .build());
        assertEquals(1, responseStreamObserver.errors().size());
        StatusRuntimeException exception = (StatusRuntimeException) responseStreamObserver.errors().get(0);
        assertEquals(ErrorCode.EXCEPTION_IN_INTERCEPTOR.getGrpcCode().getCode(),
                     exception.getStatus().getCode());
        assertEquals(ErrorCode.EXCEPTION_IN_INTERCEPTOR.getCode(),
                     exception.getTrailers().get(GrpcMetadataKeys.ERROR_CODE_KEY));
    }

    private static class TestSubscriptionQueryInterceptors implements SubscriptionQueryInterceptors {

        public boolean rejectRequest;
        public boolean failRequest;
        int subscriptionQueryRequestCount;
        int subscriptionQueryResponseCount;
        UUID lastUUID;

        @Override
        public SubscriptionQueryRequest subscriptionQueryRequest(SubscriptionQueryRequest subscriptionQueryRequest,
                                                                 ExecutionContext executionContext) {
            if (rejectRequest) {
                throw new MessagingPlatformException(ErrorCode.SUBSCRIPTION_QUERY_REJECTED_BY_INTERCEPTOR, "Rejected");
            }
            if (failRequest) {
                throw new MessagingPlatformException(ErrorCode.EXCEPTION_IN_INTERCEPTOR, "Failed");
            }
            subscriptionQueryRequestCount++;
            executionContext.putAttribute("RequestId", UUID.randomUUID());
            return subscriptionQueryRequest;
        }

        @Override
        public SubscriptionQueryResponse subscriptionQueryResponse(SubscriptionQueryResponse subscriptionQueryResponse,
                                                                   ExecutionContext executionContext) {
            subscriptionQueryResponseCount++;
            lastUUID = executionContext.getAttribute("RequestId");
            return subscriptionQueryResponse;
        }
    }

    @Test
    public void onErrorTest() {
        List<Object> events = new LinkedList<>();
        testSubject = new SubscriptionQueryRequestTarget("context",
                                                         null,
                                                         responseStreamObserver,
                                                         interceptors,
                                                         events::add);
        testSubject.consume(SubscriptionQueryRequest.newBuilder().setSubscribe(SubscriptionQuery.getDefaultInstance())
                                                    .build());

        assertEquals(1, events.size());
        Object subscriptionRequested = events.get(0);
        assertTrue(subscriptionRequested instanceof SubscriptionQueryEvents.SubscriptionQueryRequested);
        Throwable error = new RuntimeException("something happened during subscribe");
        ((SubscriptionQueryEvents.SubscriptionQueryRequested) subscriptionRequested).errorHandler().accept(error);
        assertEquals(2, events.size());
        Object subscriptionCancelled = events.get(1);
        assertTrue(subscriptionCancelled instanceof SubscriptionQueryEvents.SubscriptionQueryCanceled);
    }
}