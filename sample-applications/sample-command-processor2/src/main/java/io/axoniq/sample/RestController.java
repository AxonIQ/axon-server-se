package io.axoniq.sample;

import org.axonframework.commandhandling.gateway.CommandGateway;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * Author: marc
 */
@org.springframework.web.bind.annotation.RestController
@RequestMapping("/cmd")
public class RestController {

    private final CommandGateway commandGateway;

    public RestController(CommandGateway commandGateway) {
        this.commandGateway = commandGateway;
    }

    @RequestMapping("echo")
    public Future<String> echo(@RequestParam(value="text") String text) {
        CompletableFuture<String> result = new CompletableFuture<>();
        commandGateway.send(new EchoCommand(UUID.randomUUID().toString(), text)).whenComplete((r, t) -> result.complete(String.valueOf(r)));
        return result;
    }
}
