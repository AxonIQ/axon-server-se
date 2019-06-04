package io.axoniq.axonserver.enterprise.storage.spanner;

import com.google.protobuf.ByteString;
import io.axoniq.axonserver.enterprise.storage.spanner.serializer.ProtoMetaDataSerializer;
import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.localstorage.EventType;
import io.axoniq.axonserver.localstorage.EventTypeContext;
import io.axoniq.axonserver.localstorage.SerializedEvent;
import org.junit.*;

import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;

import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class SpannerEventStorageEngineTest {

    private SpannerEventStorageEngine testSubject;

    @Before
    public void setup() {
        StorageProperties storageProperties = new StorageProperties();
        storageProperties.setInstance("giftcard-spanner");
        storageProperties.setProjectId("giftcard-endurancetest");
        testSubject = new SpannerEventStorageEngine(new EventTypeContext("demo", EventType.EVENT),
                                                    storageProperties, new ProtoMetaDataSerializer(), s -> true);
        testSubject.init(true);
        testSubject.deleteAllEventData();
    }

    @Test
    public void addEvents() {
        testSubject.store(Arrays.asList(serializedEvent("Aggregate1", "AggregateType", 0, "Payload", "payload")));

        Optional<Long> seqNr = testSubject.getLastSequenceNumber("Aggregate1");
        assertTrue(seqNr.isPresent());
        assertEquals(Optional.of(0L), seqNr);

        testSubject.processEventsPerAggregate("Aggregate1", 0, 100, 10, new Consumer<SerializedEvent>() {
            @Override
            public void accept(SerializedEvent serializedEvent) {
                System.out.println(serializedEvent.getIdentifier());
            }
        });

    }

    private SerializedEvent serializedEvent(String aggregateId, String aggregateType, int seqNr, String payloadType, String payload) {
        Event event = Event.newBuilder().setMessageIdentifier(UUID.randomUUID().toString())
             .setTimestamp(System.currentTimeMillis())
             .setAggregateType(aggregateType)
             .setAggregateSequenceNumber(seqNr)
             .setPayload(SerializedObject.newBuilder()
             .setType(payloadType)
             .setData(ByteString.copyFrom(payload.getBytes())))
             .setAggregateIdentifier(aggregateId).build();
        return new SerializedEvent(event);
    }
}