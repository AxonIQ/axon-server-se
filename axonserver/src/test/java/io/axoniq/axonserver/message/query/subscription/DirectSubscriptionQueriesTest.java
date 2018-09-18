package io.axoniq.axonserver.message.query.subscription;

import io.axoniq.axonhub.SubscriptionQuery;
import io.axoniq.axonserver.SubscriptionQueryEvents.SubscriptionQueryCanceled;
import io.axoniq.axonserver.SubscriptionQueryEvents.SubscriptionQueryRequested;
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
        subscriptions.on(new SubscriptionQueryRequested(context, request, null, null));
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
        subscriptions.on(new SubscriptionQueryRequested(context, request, null, null));
        assertTrue(subscriptions.iterator().hasNext());

        SubscriptionQuery otherRequest = SubscriptionQuery.newBuilder().setSubscriptionIdentifier("otherID").build();
        subscriptions.on(new SubscriptionQueryCanceled(context, otherRequest));
        assertTrue(subscriptions.iterator().hasNext());
        subscriptions.on(new SubscriptionQueryCanceled(context, request));
        assertFalse(subscriptions.iterator().hasNext());
    }

}