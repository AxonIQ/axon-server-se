package io.axoniq.axonserver.enterprise.storage.jdbc;

import io.axoniq.axonserver.enterprise.storage.jdbc.multicontext.SingleSchemaMultiContextStrategy;
import io.axoniq.axonserver.enterprise.storage.jdbc.serializer.ProtoMetaDataSerializer;
import io.axoniq.axonserver.enterprise.storage.jdbc.specific.H2Specific;
import io.axoniq.axonserver.enterprise.storage.jdbc.sync.StoreAlwaysSyncStrategy;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.localstorage.EventType;
import io.axoniq.axonserver.localstorage.EventTypeContext;
import io.axoniq.axonserver.localstorage.SerializedEvent;
import io.axoniq.axonserver.localstorage.SerializedTransactionWithToken;
import junit.framework.TestCase;
import org.junit.*;

import java.util.Arrays;
import java.util.Iterator;
import java.util.UUID;

/**
 * @author Marc Gathier
 */
public class JdbcEventStorageEngineTest {
    private JdbcEventStorageEngine jdbcEventStore;

    @Before
    public void setUp()  {
        jdbcEventStore = new JdbcEventStorageEngine(new EventTypeContext("DEMO", EventType.EVENT),
                                            new StorageProperties().dataSource(),
                                            new ProtoMetaDataSerializer(),
                                            new SingleSchemaMultiContextStrategy(new H2Specific()),
                                            new StoreAlwaysSyncStrategy());
        jdbcEventStore.init(false);
    }

    @After
    public void tearDown()  {
        jdbcEventStore.deleteAllEventData();
    }

    @Test
    public void transactionIterator() {
        jdbcEventStore.store(Arrays.asList(serializedEvent("DemoType","DEMO", 0)));

        Iterator<SerializedTransactionWithToken> iterator = jdbcEventStore
                .transactionIterator(0, 100);

        while( iterator.hasNext()) {
            SerializedTransactionWithToken transactionWithToken = iterator.next();
            TestCase.assertEquals(0, transactionWithToken.getToken());
        }

        jdbcEventStore.store(Arrays.asList(serializedEvent("DemoType","DEMO", 1)));

        TestCase.assertTrue(iterator.hasNext());
        TestCase.assertEquals(1, iterator.next().getToken());



    }

    private SerializedEvent serializedEvent(String aggregateType, String aggregateId, int aggregateSeq) {
        return new SerializedEvent(Event.newBuilder()
                                        .setAggregateSequenceNumber(aggregateSeq)
                                        .setAggregateIdentifier(aggregateId)
                                        .setAggregateType(aggregateType)
                                        .setTimestamp(System.currentTimeMillis())
                                        .setMessageIdentifier(UUID.randomUUID().toString())
                                        .build());
    }

    @Test
    public void prepareTransaction() {
    }

    @Test
    public void store() {
    }

    @Test
    public void getGlobalIterator() {
    }
}