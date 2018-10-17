package io.axoniq.sample;


import org.axonframework.eventhandling.DomainEventMessage;
import org.axonframework.eventhandling.GenericDomainEventMessage;
import org.axonframework.eventsourcing.eventstore.DomainEventStream;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

/**
 * Author: marc
 */
@RestController
@RequestMapping("/snapshot")
public class SnapshotRestController {

    private final EventStore eventStore;

    public SnapshotRestController(EventStore eventStore) {
        this.eventStore = eventStore;
    }

    @GetMapping("/read")
    public List<String> snapshots(@RequestParam(value="aggregateIdentifier") String aggregateIdentifier) {
        List<String> result = new ArrayList<>();
        DomainEventStream events = eventStore
                .readEvents(aggregateIdentifier);
        while(events.hasNext()) {
            DomainEventMessage<?> event = events.next();
            result.add(event.getType() + " " + event.getSequenceNumber());
        }
        return result;
    }

    @GetMapping("/create")
    public void snapshots(@RequestParam(value="aggregateIdentifier") String aggregateIdentifier,
                          @RequestParam(value="sequenceNumber") int sequenceNumber) {
        SampleCommandHandler snapshot = new SampleCommandHandler();
        snapshot.setId(aggregateIdentifier);

        eventStore.storeSnapshot(new GenericDomainEventMessage<>(SampleCommandHandler.class.getTypeName(), aggregateIdentifier, sequenceNumber, snapshot));

    }

}
