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
import io.axoniq.axonserver.grpc.query.SubscriptionQuery;
import io.axoniq.axonserver.grpc.query.SubscriptionQueryRequest;
import io.axoniq.axonserver.grpc.query.SubscriptionQueryResponse;
import io.axoniq.axonserver.test.FakeStreamObserver;
import org.junit.*;

import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class SubscriptionQueryRequestTargetTest {

    private final FakeStreamObserver<SubscriptionQueryResponse> responseStreamObserver = new FakeStreamObserver<>();

    @Test
    public void onErrorTest() {
        List<Object> events = new LinkedList<>();
        SubscriptionQueryRequestTarget testSubject = new SubscriptionQueryRequestTarget("context",
                                                                                        responseStreamObserver,
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