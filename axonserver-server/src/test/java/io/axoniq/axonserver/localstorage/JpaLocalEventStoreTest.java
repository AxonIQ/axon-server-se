package io.axoniq.axonserver.localstorage;

import io.axoniq.axondb.Event;
import io.axoniq.axondb.grpc.GetAggregateEventsRequest;
import io.axoniq.axonserver.enterprise.storage.jdbc.JdbcEventStoreFactory;
import io.axoniq.axonserver.enterprise.storage.jdbc.StorageProperties;
import io.axoniq.platform.SerializedObject;
import io.grpc.stub.StreamObserver;
import org.junit.*;

import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

import static junit.framework.TestCase.assertEquals;

/**
 * Author: marc
 */
public class JpaLocalEventStoreTest {
    private LocalEventStore testSubject;

    @Before
    public void setup() {
        testSubject = new LocalEventStore(new JdbcEventStoreFactory(new StorageProperties()));
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

        assertEquals(eventsPerAggregate, responseObserver.messages.size());
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

        assertEquals(8, responseObserver.messages.size());
    }



    private String[] storeEvents(int aggregeteIdCount, int eventsPerAggregate) {
        String[] aggregateIds = new String[aggregeteIdCount];
        IntStream.range(0, aggregeteIdCount).parallel().forEach(j -> {
            aggregateIds[j] = UUID.randomUUID().toString();
            StreamObserver<Event> connection = testSubject.createAppendEventConnection("default",
                                                                                             new CollectingStreamObserver<>());
            IntStream.range(0, eventsPerAggregate).forEach(i -> {
                Event event = Event.newBuilder().setAggregateIdentifier(aggregateIds[j]).setAggregateSequenceNumber(i)
                                   .setMessageIdentifier(UUID.randomUUID().toString())
                                   .setAggregateType("Demo").setPayload(SerializedObject.newBuilder().build()).build();
                connection.onNext(event);
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
