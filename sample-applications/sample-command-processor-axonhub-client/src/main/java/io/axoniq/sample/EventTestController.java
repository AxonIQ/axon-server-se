package io.axoniq.sample;

import org.axonframework.eventhandling.EventBus;
import org.axonframework.eventsourcing.eventstore.TrackingToken;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;

/**
 * @author Marc Gathier
 */
@RestController
@RequestMapping("/event")
public class EventTestController {

    private final EventBus eventBus;

    public EventTestController(EventBus eventBus) {
        this.eventBus = eventBus;
    }

    @GetMapping("/tail")
    public TrackingToken first() {
        return eventBus.createTailToken();
    }

    @GetMapping("/head")
    public TrackingToken last() {
        return eventBus.createHeadToken();
    }

    @GetMapping("/at")
    public TrackingToken tokenAt(@RequestParam("time") String text) {
        return eventBus.createTokenAt(Instant.parse(text));
    }
}
