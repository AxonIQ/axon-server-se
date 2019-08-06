package io.axoniq.axonserver.rest;


import io.axoniq.axonserver.topology.DefaultEventStoreLocator;
import io.axoniq.axonserver.topology.EventStoreLocator;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Rest calls for convenience in development/test environments. These endpoints are only available when development
 * mode is active.
 * @author Greg Woods
 * @since 4.2
 */
@RestController("DevelopmentRestController")
@ConditionalOnMissingBean(name = {"axonServerEnterprise"})
@ConditionalOnProperty("axoniq.axonserver.devmode.enabled")
@RequestMapping("/v1/devmode")
public class DevelopmentRestController {

    @Autowired
    private EventStoreLocator eventStoreLocator;

    /**
     * REST endpoint handling requests to reset the events and snapshots in Axon Server
     */
    @DeleteMapping("purge-events")
    @ApiOperation(value="Clears all event and snapshot data from Axon Server", notes = "Only for development/test environments.")
    public void resetEventStore(){
        eventStoreLocator.getEventStore("default").deleteAllEventData("default");
    }
}
