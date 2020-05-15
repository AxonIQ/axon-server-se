package io.axoniq.sample;

import org.axonframework.commandhandling.gateway.CommandGateway;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventhandling.GenericEventMessage;
import org.axonframework.eventhandling.scheduling.EventScheduler;
import org.axonframework.eventhandling.scheduling.java.SimpleScheduleToken;
import org.axonframework.messaging.MetaData;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * @author Marc Gathier
 */
@org.springframework.web.bind.annotation.RestController
@RequestMapping("/cmd")
public class RestController {

    private final CommandGateway commandGateway;
    private final EventScheduler eventScheduler;

    public RestController(CommandGateway commandGateway,
                          EventScheduler eventScheduler) {
        this.commandGateway = commandGateway;
        this.eventScheduler = eventScheduler;
    }

    @RequestMapping("echo")
    public Future<String> echo(@RequestParam(value = "text") String text) {
        CompletableFuture<String> result = new CompletableFuture<>();
        commandGateway.send(new EchoCommand(UUID.randomUUID().toString(), text)).whenComplete((r, t) -> {
            if (t != null) {
                result.complete(t.getCause().getMessage());
            } else {
                result.complete(String.valueOf(r));
            }
        });
        return result;
    }

    @RequestMapping("schedule")
    public String schedule(@RequestParam(value = "text") String text,
                           @RequestParam(value = "after", defaultValue = "10") int after) {
        EventMessage<String> eventMessage = new GenericEventMessage<>(text, MetaData.with("Sample", 12425535));
        return eventScheduler.schedule(Duration.ofSeconds(after), eventMessage).toString();
    }

    @RequestMapping("cancel")
    public void cancel(@RequestParam(value = "taskId") String taskId) {
        eventScheduler.cancelSchedule(new SimpleScheduleToken(taskId));
    }

    @RequestMapping("reschedule")
    public void reschedule(@RequestParam(value = "taskId") String taskId, @RequestParam(value = "text") String text,
                           @RequestParam(value = "after", defaultValue = "10") int after) {
        EventMessage<String> eventMessage = new GenericEventMessage<>(text, MetaData.with("Sample", 12425535));
        eventScheduler.reschedule(new SimpleScheduleToken(taskId), Duration.ofSeconds(after), eventMessage);
    }
}
