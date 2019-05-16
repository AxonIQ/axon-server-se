package io.axoniq.axonserver.enterprise.storage.jdbc;

import io.axoniq.axonserver.enterprise.storage.jdbc.multicontext.SingleSchemaMultiContextStrategy;
import io.axoniq.axonserver.enterprise.storage.jdbc.serializer.ProtoMetaDataSerializer;
import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.GetAggregateEventsRequest;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import io.axoniq.axonserver.localstorage.transaction.SingleInstanceTransactionManager;
import io.grpc.stub.StreamObserver;
import junit.framework.TestCase;
import org.junit.*;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

/**
 * @author Marc Gathier
 */
public class JpaLocalEventStoreTest {
    private LocalEventStore testSubject;

    @Before
    public void setup() throws SQLException {
        StorageProperties storageProperties = new StorageProperties();
        testSubject = new LocalEventStore(new JdbcEventStoreFactory(storageProperties,
                                                                    SingleInstanceTransactionManager::new,
                                                                    new ProtoMetaDataSerializer(),
                                                                    new SingleSchemaMultiContextStrategy(storageProperties.getVendorSpecific()),
                                                                    null));
        testSubject.initContext("default", false);
    }

    @Test
    public void readAggregate() {
        int aggregeteIdCount = 5;
        int eventsPerAggregate = 5;
        String[] aggregateIds = storeEvents(aggregeteIdCount, eventsPerAggregate);



        GetAggregateEventsRequest request = GetAggregateEventsRequest.newBuilder().setAggregateId(aggregateIds[0])
                                                                     .build();

        CollectingStreamObserver<InputStream> responseObserver = new CollectingStreamObserver<>();
        testSubject.listAggregateEvents("default", request, responseObserver);

        TestCase.assertEquals(eventsPerAggregate, responseObserver.messages.size());
    }

    @Test
    public void readAggregateWithSnapshot() throws ExecutionException, InterruptedException {
        int aggregeteIdCount = 1;
        int eventsPerAggregate = 15;
        String[] aggregateIds = storeEvents(aggregeteIdCount, eventsPerAggregate);
        Event event = Event.newBuilder().setAggregateIdentifier(aggregateIds[0]).setAggregateSequenceNumber(7)
                           .setMessageIdentifier(UUID.randomUUID().toString())
                           .setAggregateType("Snapshot").setPayload(SerializedObject.newBuilder().build()).build();
        testSubject.appendSnapshot("default", event).get();

        GetAggregateEventsRequest request = GetAggregateEventsRequest.newBuilder().setAggregateId(aggregateIds[0])
                                                                     .setAllowSnapshots(true)
                                                                     .build();

        CollectingStreamObserver<InputStream> responseObserver = new CollectingStreamObserver<>();
        testSubject.listAggregateEvents("default", request, responseObserver);

        TestCase.assertEquals(8, responseObserver.messages.size());
    }



    private String[] storeEvents(int aggregeteIdCount, int eventsPerAggregate) {
        String[] aggregateIds = new String[aggregeteIdCount];
        IntStream.range(0, aggregeteIdCount).parallel().forEach(j -> {
            aggregateIds[j] = UUID.randomUUID().toString();
            StreamObserver<InputStream> connection = testSubject.createAppendEventConnection("default",
                                                                                             new CollectingStreamObserver<>());
            IntStream.range(0, eventsPerAggregate).forEach(i -> {
                Event event = Event.newBuilder().setAggregateIdentifier(aggregateIds[j]).setAggregateSequenceNumber(i)
                                   .setMessageIdentifier(UUID.randomUUID().toString())
                                   .setAggregateType("Demo").setPayload(SerializedObject.newBuilder().build()).build();
                connection.onNext(new ByteArrayInputStream(event.toByteArray()));
            });
            connection.onCompleted();
        });
        return aggregateIds;
    }


    private class CollectingStreamObserver<T> implements StreamObserver<T> {
        private List<T> messages = new LinkedList<>();
        private boolean success;
        private Throwable error;

        @Override
        public void onNext(T confirmation) {
            messages.add(confirmation);
        }

        @Override
        public void onError(Throwable throwable) {
            this.error = throwable;

        }

        @Override
        public void onCompleted() {
            this.success = true;

        }
    }
}
