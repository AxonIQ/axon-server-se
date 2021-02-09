/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.query.subscription;

import io.axoniq.axonserver.applicationevents.SubscriptionEvents.SubscribeQuery;
import io.axoniq.axonserver.applicationevents.SubscriptionQueryEvents.SubscriptionQueryCanceled;
import io.axoniq.axonserver.applicationevents.SubscriptionQueryEvents.SubscriptionQueryInitialResultRequested;
import io.axoniq.axonserver.applicationevents.SubscriptionQueryEvents.SubscriptionQueryRequested;
import io.axoniq.axonserver.applicationevents.TopologyEvents;
import io.axoniq.axonserver.grpc.query.QueryProviderInbound;
import io.axoniq.axonserver.grpc.query.QueryRequest;
import io.axoniq.axonserver.grpc.query.QuerySubscription;
import io.axoniq.axonserver.grpc.query.SubscriptionQuery;
import io.axoniq.axonserver.grpc.query.SubscriptionQueryRequest;
import io.axoniq.axonserver.message.ClientStreamIdentification;
import io.axoniq.axonserver.message.query.QueryHandler;
import io.axoniq.axonserver.message.query.QueryRegistrationCache;
import io.axoniq.axonserver.test.FakeStreamObserver;
import org.junit.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import javax.annotation.Nonnull;

import static io.axoniq.axonserver.grpc.query.SubscriptionQueryRequest.RequestCase.*;
import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 * @since 4.1
 */
public class SubscriptionQueryDispatcherTest {

    public static final String COMPONENT = "component";
    private final String context = "Demo";
    private final SubscriptionQuery subscriptionQuery =
            SubscriptionQuery.newBuilder()
                             .setQueryRequest(QueryRequest.newBuilder().setQuery("test"))
                             .build();
    private QueryRegistrationCache cache;
    private SubscriptionQueryDispatcher testSubject;

    @Before
    public void init() {
        cache = new QueryRegistrationCache((queryDefinition, componentName, queryHandlers) -> queryHandlers.first());
        testSubject = new SubscriptionQueryDispatcher(this::getDirectSubscriptions, cache);
    }

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
        String client = "client";
        SubscribeQuery subscribeQuery = subscribeQuery(request -> dispatchedSubscriptions.incrementAndGet(), client);
        testSubject.on(subscribeQuery);
        assertEquals(1, dispatchedSubscriptions.get());
        testSubject.on(new TopologyEvents.QueryHandlerDisconnected(context, client, client + "StreamId"));
        testSubject.on(subscribeQuery);
        assertEquals(2, dispatchedSubscriptions.get());
    }

    @Nonnull
    private SubscribeQuery subscribeQuery(Consumer<SubscriptionQueryRequest> requestConsumer, String client) {
        final String clientStreamId = client + "StreamId";
        QueryHandler<QueryProviderInbound> queryHandler = new QueryHandler<QueryProviderInbound>(
                new FakeStreamObserver<>(),
                new ClientStreamIdentification(context, clientStreamId), COMPONENT, client) {
            @Override
            public void dispatch(SubscriptionQueryRequest query) {
                requestConsumer.accept(query);
            }
        };
        return new SubscribeQuery(context,
                                  clientStreamId,
                                  QuerySubscription.newBuilder().setClientId(client).setQuery("test").build(),
                                  queryHandler);
    }

    @Test
    public void onInitialResultRequest() {
        AtomicInteger count = new AtomicInteger(0);
        Consumer<SubscriptionQueryRequest> requestConsumer = request -> {
            if (request.getRequestCase().equals(GET_INITIAL_RESULT)) {
                count.incrementAndGet();
            }
        };
        cache.on(subscribeQuery(requestConsumer, "client1"));
        cache.on(subscribeQuery(requestConsumer, "client2"));
        cache.on(subscribeQuery(requestConsumer, "client3"));
        cache.on(subscribeQuery(requestConsumer, "client4"));
        testSubject.on(new SubscriptionQueryInitialResultRequested(context,
                                                                   subscriptionQuery,
                                                                   response -> {
                                                                   },
                                                                   error -> {
                                                                   }));
        assertEquals(1, count.get());
    }

    @Test
    public void onSubscribe() {
        AtomicInteger count = new AtomicInteger(0);
        Consumer<SubscriptionQueryRequest> requestConsumer = request -> {
            if (request.getRequestCase().equals(SUBSCRIBE)) {
                count.incrementAndGet();
            }
        };
        cache.on(subscribeQuery(requestConsumer, "client1"));
        cache.on(subscribeQuery(requestConsumer, "client2"));
        cache.on(subscribeQuery(requestConsumer, "client3"));
        cache.on(subscribeQuery(requestConsumer, "client4"));
        testSubject.on(new SubscriptionQueryRequested(context, subscriptionQuery, response -> {
        }, throwable -> {
        }));
        assertEquals(4, count.get());
    }

    @Test
    public void onUnsubscribe() {
        AtomicInteger count = new AtomicInteger(0);
        Consumer<SubscriptionQueryRequest> requestConsumer = request -> {
            if (request.getRequestCase().equals(UNSUBSCRIBE)) {
                count.incrementAndGet();
            }
        };
        cache.on(subscribeQuery(requestConsumer, "client1"));
        cache.on(subscribeQuery(requestConsumer, "client2"));
        cache.on(subscribeQuery(requestConsumer, "client3"));
        cache.on(subscribeQuery(requestConsumer, "client4"));
        testSubject.on(new SubscriptionQueryCanceled(context, subscriptionQuery));
        assertEquals(4, count.get());
    }
}