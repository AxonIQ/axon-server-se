package io.axoniq.sample;

import org.axonframework.commandhandling.gateway.CommandGateway;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

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
        StringBuilder builder = new StringBuilder();
        IntStream.range(0, 10000000).forEach(i -> builder.append(text));
        commandGateway.send(new EchoCommand(UUID.randomUUID().toString(), builder.toString())).whenComplete((r, t) -> result.complete(String.valueOf(r)));
        return result;
    }
}
