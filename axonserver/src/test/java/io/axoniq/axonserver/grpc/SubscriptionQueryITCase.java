package io.axoniq.axonserver.grpc;

import org.axonframework.axonserver.connector.AxonServerConfiguration;
import org.axonframework.axonserver.connector.AxonServerConnectionManager;
import org.axonframework.axonserver.connector.query.AxonServerQueryBus;
import org.axonframework.axonserver.connector.query.QueryPriorityCalculator;
import org.axonframework.common.Registration;
import org.axonframework.messaging.Message;
import org.axonframework.messaging.responsetypes.ResponseTypes;
import org.axonframework.queryhandling.GenericSubscriptionQueryMessage;
import org.axonframework.queryhandling.QueryBus;
import org.axonframework.queryhandling.QueryResponseMessage;
import org.axonframework.queryhandling.SimpleQueryBus;
import org.axonframework.queryhandling.SubscriptionQueryResult;
import org.axonframework.queryhandling.SubscriptionQueryUpdateMessage;
import org.axonframework.serialization.xml.XStreamSerializer;
import org.junit.*;

import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;

import static org.junit.Assert.*;

/**
 * Integration test for Subscription Queries
 *
 * @author Sara Pellegrini
 * @since 4.2
 */

public class SubscriptionQueryITCase {

    private final AxonServerConfiguration configuration = AxonServerConfiguration.builder().build();

    private final AxonServerConnectionManager connectionManager = new AxonServerConnectionManager(configuration);

    private final SimpleQueryBus queryBus = SimpleQueryBus.builder().build();

    private final QueryBus axonServerQueryBus = new AxonServerQueryBus(connectionManager,
                                                                       configuration,
                                                                       queryBus.queryUpdateEmitter(),
                                                                       queryBus,
                                                                       XStreamSerializer.defaultSerializer(),
                                                                       XStreamSerializer.defaultSerializer(),
                                                                       QueryPriorityCalculator
                                                                               .defaultQueryPriorityCalculator());

    @Test
    public void testMemoryLeak() throws InterruptedException {
        Registration echo = axonServerQueryBus.subscribe("test", String.class, Message::getPayload);
        ReferenceQueue<Object> referenceQueue = new ReferenceQueue<>();
        Reference<Object> ref = new PhantomReference<>(invokeQuery(), referenceQueue);
        queryBus.queryUpdateEmitter().emit(String.class, c -> true, "update");
        assertFalse(checkObjectGarbageCollected(ref, referenceQueue));
        queryBus.queryUpdateEmitter().complete(String.class, c -> true);
        System.gc();
        Thread.sleep(1000);
        System.gc();
        assertTrue(checkObjectGarbageCollected(ref, referenceQueue));
        echo.cancel();
        connectionManager.shutdown();
    }

    private SubscriptionQueryResult invokeQuery() {
        GenericSubscriptionQueryMessage<String, String, String> query =
                new GenericSubscriptionQueryMessage<>("hi",
                                                      "test",
                                                      ResponseTypes.instanceOf(String.class),
                                                      ResponseTypes.instanceOf(String.class));
        SubscriptionQueryResult<QueryResponseMessage<String>, SubscriptionQueryUpdateMessage<String>> result = queryBus
                .subscriptionQuery(query);

        result.updates()
              .subscribe(
                      System.out::println,
                      System.out::println,
                      () -> System.out.println("completed"));

        return result;
    }

    private boolean checkObjectGarbageCollected(Reference<Object> ref, ReferenceQueue<Object> referenceQueue) {
        Reference<?> polledRef = referenceQueue.poll();
        return polledRef == ref;
    }
}
