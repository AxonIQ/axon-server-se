package io.axoniq.axonserver.rest;


import io.swagger.annotations.ApiOperation;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Rest calls for convenience in development/test environments
 * @author Greg Woods
 */
@RestController("DevelopmentRestController")
@ConditionalOnProperty("axoniq.axonserver.devtools.enabled")
@RequestMapping("/v1/devtools")
public class DevelopmentRestController {

    @DeleteMapping("delete-events")
    @ApiOperation(value="Clears all event data, saga data and tracking projection tokens from the cluster", notes = "Only for development/test environments.")
    public void resetEventStore() { }
}
