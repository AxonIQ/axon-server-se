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
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.axoniq.axonserver.test.AssertUtils.assertWithin;
import static org.junit.Assert.*;

/**
 * Integration test for Subscription Queries
 *
 * @author Sara Pellegrini
 * @since 4.2
 */
public class SubscriptionQueryITCase {

    private final AxonServerConfiguration configuration = AxonServerConfiguration.builder().build();

    private final AxonServerConnectionManager connectionManager =
            AxonServerConnectionManager.builder().axonServerConfiguration(configuration).build();

    private final SimpleQueryBus queryBus = SimpleQueryBus.builder().build();

    private final QueryBus axonServerQueryBus = AxonServerQueryBus.builder()
                                                                  .axonServerConnectionManager(connectionManager)
                                                                  .configuration(configuration)
                                                                  .updateEmitter(queryBus.queryUpdateEmitter())
                                                                  .localSegment(queryBus)
                                                                  .genericSerializer(XStreamSerializer
                                                                                             .defaultSerializer())
                                                                  .messageSerializer(XStreamSerializer
                                                                                             .defaultSerializer())
                                                                  .priorityCalculator(QueryPriorityCalculator
                                                                                              .defaultQueryPriorityCalculator())
                                                                  .build();

    private final List<String> initialResult = new ArrayList<>();
    private final List<String> updates = new ArrayList<>();

    @Test
    public void testMemoryLeak() throws InterruptedException {
        Registration echo = axonServerQueryBus.subscribe("test", String.class, Message::getPayload);
        ReferenceQueue<Object> referenceQueue = new ReferenceQueue<>();
        Reference<Object> ref = new PhantomReference<>(invokeQuery(), referenceQueue);
        queryBus.queryUpdateEmitter().emit(String.class, c -> true, "update");
        assertNull(referenceQueue.poll());
        queryBus.queryUpdateEmitter().complete(String.class, c -> true);
        //this sleep is needed to wait the gRPC call to be close
        Thread.sleep(1000);
        System.gc();
        assertWithin(20, TimeUnit.MILLISECONDS, () -> assertEquals(ref, referenceQueue.poll()));
        assertEquals(Collections.singletonList("hi"), initialResult);
        assertEquals(Collections.singletonList("update"), updates);
        echo.cancel();
        connectionManager.shutdown();
    }

    private Object invokeQuery() {
        GenericSubscriptionQueryMessage<String, String, String> query =
                new GenericSubscriptionQueryMessage<>("hi",
                                                      "test",
                                                      ResponseTypes.instanceOf(String.class),
                                                      ResponseTypes.instanceOf(String.class));
        SubscriptionQueryResult<QueryResponseMessage<String>, SubscriptionQueryUpdateMessage<String>> result =
                axonServerQueryBus.subscriptionQuery(query);
        initialResult.add(result.initialResult().block(Duration.ofSeconds(2)).getPayload());
        result.updates().subscribe(message -> updates.add(message.getPayload()));
        return result;
    }

}
