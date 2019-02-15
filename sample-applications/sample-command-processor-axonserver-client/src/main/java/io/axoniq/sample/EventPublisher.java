package io.axoniq.sample;

import org.axonframework.eventhandling.EventBus;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventhandling.GenericDomainEventMessage;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.UUID;
import java.util.stream.IntStream;

/**
 * @author Marc Gathier
 */
@org.springframework.web.bind.annotation.RestController
@RequestMapping("/publish")
public class EventPublisher {
    private final EventBus eventBus;

    public EventPublisher(EventBus eventBus) {
        this.eventBus = eventBus;
    }

    @RequestMapping("events")
    public void echo(@RequestParam(value="count", defaultValue = "1", required = false) int count) {
        EventMessage<?>[] events = new EventMessage[count];
        String id = UUID.randomUUID().toString();
        events[0] = new GenericDomainEventMessage<EchoEvent>(SampleCommandHandler.class.getName(), id, 0, new EchoEvent(id, "hello"));
        IntStream.range(1, count).forEach(i -> {
            events[i] = new GenericDomainEventMessage<UpdateEvent>(SampleCommandHandler.class.getName(), id, i, new UpdateEvent(id, "hello: " + i));
        });
        eventBus.publish(events);
    }

}
