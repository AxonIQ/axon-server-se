package io.axoniq.axonserver.grpc;

import org.axonframework.axonserver.connector.AxonServerConfiguration;
import org.axonframework.axonserver.connector.AxonServerConnectionManager;
import org.axonframework.axonserver.connector.event.axon.AxonServerEventStore;
import org.axonframework.eventhandling.DomainEventMessage;
import org.axonframework.eventhandling.GenericDomainEventMessage;
import org.axonframework.eventsourcing.eventstore.DomainEventStream;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.junit.*;

import java.util.UUID;

import static org.junit.Assert.*;

/**
 * @author Sara Pellegrini
 * @since 4.5.5
 */
public class LargeAggregateITCase {

    private final AxonServerConfiguration configuration = AxonServerConfiguration.builder().build();

    private final AxonServerConnectionManager connectionManager =
            AxonServerConnectionManager.builder().axonServerConfiguration(configuration).build();


    private final EventStore axonServerEventStore = AxonServerEventStore.builder()
                                                                        .platformConnectionManager(connectionManager)
                                                                        .configuration(configuration)
                                                                        .build();


    @Test
    public void testLargeAggregate() {
        String myAggregateId = UUID.randomUUID().toString();
        for (long i = 0; i <= 100_000; i++) {
            axonServerEventStore.publish(new GenericDomainEventMessage<>("EventType",
                                                                         myAggregateId,
                                                                         i,
                                                                         "payload"));
        }

        DomainEventStream eventStream = axonServerEventStore.readEvents(myAggregateId, 0);
        long i = -1;
        while (eventStream.hasNext()) {
            DomainEventMessage<?> event = eventStream.next();
            if ((i + 1) != event.getSequenceNumber()) {
                throw new RuntimeException("Invalid sequence number");
            }
            i = event.getSequenceNumber();
        }
        assertEquals(100_000, i);
    }
}
