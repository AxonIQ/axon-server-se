/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.query.subscription;

import io.axoniq.axonserver.applicationevents.SubscriptionQueryEvents.SubscriptionQueryCanceled;
import io.axoniq.axonserver.applicationevents.SubscriptionQueryEvents.SubscriptionQueryStarted;
import io.axoniq.axonserver.grpc.query.SubscriptionQuery;
import io.axoniq.axonserver.message.query.subscription.DirectSubscriptionQueries.ContextSubscriptionQuery;
import org.junit.*;

import java.util.Iterator;

import static org.junit.Assert.*;

/**
 * Created by Sara Pellegrini on 16/05/2018.
 * sara.pellegrini@gmail.com
 */
public class DirectSubscriptionQueriesTest {

    @Test
    public void onSubscriptionQueryRequested() {
        String context = "context";
        DirectSubscriptionQueries subscriptions = new DirectSubscriptionQueries();
        assertFalse(subscriptions.iterator().hasNext());
        SubscriptionQuery request = SubscriptionQuery.newBuilder().build();
        subscriptions.on(new SubscriptionQueryStarted(context, request, null, null));
        Iterator<ContextSubscriptionQuery> iterator = subscriptions.iterator();
        assertTrue(iterator.hasNext());
        assertEquals(request, iterator.next().subscriptionQuery());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void onSubscriptionQueryCanceled() {
        String context = "context";
        DirectSubscriptionQueries subscriptions = new DirectSubscriptionQueries();
        SubscriptionQuery request = SubscriptionQuery.newBuilder().setSubscriptionIdentifier("ID").build();
        subscriptions.on(new SubscriptionQueryStarted(context, request, null, null));
        assertTrue(subscriptions.iterator().hasNext());

        SubscriptionQuery otherRequest = SubscriptionQuery.newBuilder().setSubscriptionIdentifier("otherID").build();
        subscriptions.on(new SubscriptionQueryCanceled(context, otherRequest));
        assertTrue(subscriptions.iterator().hasNext());
        subscriptions.on(new SubscriptionQueryCanceled(context, request));
        assertFalse(subscriptions.iterator().hasNext());
    }

}