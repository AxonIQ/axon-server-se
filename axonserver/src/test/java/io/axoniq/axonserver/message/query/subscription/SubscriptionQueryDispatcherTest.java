/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.query.subscription;

import io.axoniq.axonserver.applicationevents.SubscriptionEvents;
import io.axoniq.axonserver.applicationevents.TopologyEvents;
import io.axoniq.axonserver.grpc.query.QueryProviderInbound;
import io.axoniq.axonserver.grpc.query.QueryRequest;
import io.axoniq.axonserver.grpc.query.QuerySubscription;
import io.axoniq.axonserver.grpc.query.SubscriptionQuery;
import io.axoniq.axonserver.grpc.query.SubscriptionQueryRequest;
import io.axoniq.axonserver.message.ClientStreamIdentification;
import io.axoniq.axonserver.message.query.QueryHandler;
import io.axoniq.axonserver.message.query.QueryRegistrationCache;
import io.axoniq.axonserver.util.CountingStreamObserver;
import org.junit.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 * @since 4.1
 */
public class SubscriptionQueryDispatcherTest {

    private SubscriptionQueryDispatcher testSubject = new SubscriptionQueryDispatcher(
            this::getDirectSubscriptions,
            new QueryRegistrationCache((queryDefinition, componentName, queryHandlers) -> null));

    private Iterator<DirectSubscriptionQueries.ContextSubscriptionQuery> getDirectSubscriptions() {
        List<DirectSubscriptionQueries.ContextSubscriptionQuery> subscriptionQueries = new ArrayList<>();
        subscriptionQueries.add(new DirectSubscriptionQueries.ContextSubscriptionQuery(
                "Demo",
                SubscriptionQuery.newBuilder()
                                 .setSubscriptionIdentifier("111")
                                 .setQueryRequest(QueryRequest.newBuilder()
                                                              .setQuery("test")
                                                              .build())
                                 .build()));
        return subscriptionQueries.iterator();
    }

    @Test
    public void onQueryDisconnected() {
        AtomicInteger dispatchedSubscriptions = new AtomicInteger();
        SubscriptionEvents.SubscribeQuery subscribeQuery =
                new SubscriptionEvents.SubscribeQuery("Demo",
                                                      "clientStreamId",
                                                      QuerySubscription.newBuilder().setClientId("clientId")
                                                                       .setQuery("test").build(),
                                                      new QueryHandler<QueryProviderInbound>(
                                                              new CountingStreamObserver<>(),
                                                              new ClientStreamIdentification("Demo", "clientStreamId"),
                                                              "component", "clientId") {
                                                          @Override
                                                          public void dispatch(SubscriptionQueryRequest query) {
                                                              dispatchedSubscriptions.incrementAndGet();
                                                          }
                                                      });
        testSubject.on(subscribeQuery);
        assertEquals(1, dispatchedSubscriptions.get());
        testSubject.on(new TopologyEvents.QueryHandlerDisconnected("Demo", "clientId", "clientStreamId"));
        testSubject.on(subscribeQuery);
        assertEquals(2, dispatchedSubscriptions.get());
    }
}