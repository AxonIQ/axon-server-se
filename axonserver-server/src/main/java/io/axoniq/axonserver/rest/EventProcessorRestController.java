package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.component.ComponentItems;
import io.axoniq.axonserver.component.instance.Client;
import io.axoniq.axonserver.component.instance.Clients;
import io.axoniq.axonserver.component.processor.ApplicationProcessorEventsSource;
import io.axoniq.axonserver.component.processor.ComponentProcessors;
import io.axoniq.axonserver.component.processor.listener.ClientProcessors;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Created by Sara Pellegrini on 09/03/2018.
 * sara.pellegrini@gmail.com
 */
@RestController
@RequestMapping("v1")
public class EventProcessorRestController {

    private final ApplicationProcessorEventsSource processorEventsSource;

    private final ClientProcessors eventProcessors;

    private final Clients clients;

    public EventProcessorRestController(ApplicationProcessorEventsSource processorEventsSource,
                                        ClientProcessors eventProcessors,
                                        Clients clients) {
        this.processorEventsSource = processorEventsSource;
        this.eventProcessors = eventProcessors;
        this.clients = clients;
    }

    @GetMapping("components/{component}/processors")
    public Iterable componentProcessors(@PathVariable("component") String component,
                                        @RequestParam("context") String context) {
        return new ComponentProcessors(component, context, eventProcessors);
    }

    @PatchMapping("components/{component}/processors/{processor}/pause")
    public void pause(@PathVariable("component") String component,
                      @PathVariable("processor") String processor,
                      @RequestParam("context") String context) {
        Iterable<Client> clientIterable = new ComponentItems<>(component, context, this.clients);
        clientIterable.forEach(client -> this.processorEventsSource.pauseProcessorRequest(client.name(), processor));
    }

    @PatchMapping("components/{component}/processors/{processor}/start")
    public void start(@PathVariable("component") String component,
                      @PathVariable("processor") String processor,
                      @RequestParam("context") String context){
        Iterable<Client> clientIterable = new ComponentItems<>(component, context, this.clients);
        clientIterable.forEach(client -> this.processorEventsSource.startProcessorRequest(client.name(), processor));
    }

    @PatchMapping("components/{component}/processors/{processor}/segments/{segment}/move")
    public void moveSegment(@PathVariable("component") String component,
                            @PathVariable("processor") String processor,
                            @PathVariable("segment") int segment,
                            @RequestParam("target") String target,
                            @RequestParam("context") String context) {
        Iterable<Client> clients = new ComponentItems<>(component, context, this.clients);
        clients.forEach(client -> {
            if (!target.equals(client.name())){
                this.processorEventsSource.releaseSegment(client.name(), processor, segment);
            }
        });
    }

}
