package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.grpc.PlatformService;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Author: marc
 */
@RestController("InstructionRestController")
@RequestMapping("/v1/instructions")
public class InstructionRestController {
    private final PlatformService platformService;

    public InstructionRestController(PlatformService platformService) {
        this.platformService = platformService;
    }

    @PatchMapping
    public String requestReconnect(@RequestParam(value="client") String client) {
        if( platformService.requestReconnect(client)) {
            return client + ": requested reconnect";
        } else {
            return client +  ": not connected to this server";
        }
    }
}
