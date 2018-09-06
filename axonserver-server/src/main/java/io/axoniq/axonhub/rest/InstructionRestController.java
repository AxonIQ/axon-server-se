package io.axoniq.axonhub.rest;

import io.axoniq.axonhub.grpc.PlatformService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Author: marc
 */
@RestController("InstructionRestController")
@RequestMapping("/v1/instructions")
public class InstructionRestController {
    private final PlatformService instructionService;

    public InstructionRestController(PlatformService instructionService) {
        this.instructionService = instructionService;
    }

    @GetMapping
    public String requestReconnect(@RequestParam(value="client") String client) {
        if( instructionService.requestReconnect(client)) {
            return client + ": requested reconnect";
        } else {
            return client +  ": not connected to this server";
        }
    }
}
