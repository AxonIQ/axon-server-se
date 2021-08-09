package io.axoniq.axonserver.admin.eventprocessor.transport.rest;

import io.axoniq.axonserver.admin.eventprocessor.api.EventProcessorAdminService;
import io.axoniq.axonserver.admin.eventprocessor.api.EventProcessorId;
import io.axoniq.axonserver.admin.eventprocessor.api.EventProcessorState;
import io.axoniq.axonserver.api.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import springfox.documentation.annotations.ApiIgnore;

import java.security.Principal;

/**
 * @author Sara Pellegrini
 * @since 4.6
 */
@RestController
@RequestMapping("v1/event-processors")
public class EventProcessorAdminRestController {

    private final EventProcessorAdminService service;

    public EventProcessorAdminRestController(EventProcessorAdminService service) {
        this.service = service;
    }

    @PatchMapping("{name}/pause")
    public void pause(@PathVariable("processor") String processorName,
                      @RequestParam("context") String context,
                      @RequestParam("tokenStoreIdentifier") String tokenStoreIdentifier,
                      @ApiIgnore final Principal principal) {
        EventProcessorId eventProcessorId = new EventProcessorId(processorName, context, tokenStoreIdentifier);
        service.pause(eventProcessorId, new Authentication(principal.getName()));
    }

    @GetMapping
    public Flux<EventProcessorState> eventProcessorsPerContext(@RequestParam("context") String context,
                                                               @ApiIgnore final Principal principal) {
        return service.eventProcessorsForContext(context, new Authentication(principal.getName()));
    }
}
