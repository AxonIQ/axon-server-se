package io.axoniq.sample;

import org.axonframework.commandhandling.gateway.CommandGateway;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

/**
 * @author Marc Gathier
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
        commandGateway.send(new EchoCommand(UUID.randomUUID().toString(), text)).whenComplete((r, t) -> {
            result.complete(String.valueOf(r));
        });
        return result;
    }

    @RequestMapping("update")
    public Future<String> update(@RequestParam(value="id") String id, @RequestParam(value="text") String text) {
        CompletableFuture<String> result = new CompletableFuture<>();
        commandGateway.send(new UpdateCommand(id, text)).whenComplete((r, t) -> result.complete(String.valueOf(r)));
        return result;
    }

    @RequestMapping("batch")
    public Future<String> update(@RequestParam(value="count", defaultValue = "5")  int count) {
        CompletableFuture<String> result = new CompletableFuture<>();
        CountDownLatch resultCounter = new CountDownLatch(count);
        IntStream.range(0, count).parallel().forEach(i -> {
            commandGateway.send(new BatchCommand(UUID.randomUUID().toString(), "BatchCommand"))
                    .whenComplete((r, t) -> {
                        if( t != null) {
                            result.complete(t.getMessage());
                        }
                        resultCounter.countDown();
                        if( resultCounter.getCount() == 0) result.complete(count + " commands processed");
                    });
        });
        return result;
    }
}
