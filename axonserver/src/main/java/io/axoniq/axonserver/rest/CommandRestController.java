package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.KeepNames;
import io.axoniq.axonserver.component.ComponentItems;
import io.axoniq.axonserver.component.command.ComponentCommand;
import io.axoniq.axonserver.component.command.DefaultCommands;
import io.axoniq.axonserver.grpc.SerializedCommand;
import io.axoniq.axonserver.message.command.CommandDispatcher;
import io.axoniq.axonserver.message.command.CommandHandler;
import io.axoniq.axonserver.message.command.CommandRegistrationCache;
import io.axoniq.axonserver.rest.json.CommandRequestJson;
import io.axoniq.axonserver.rest.json.CommandResponseJson;
import io.axoniq.axonserver.topology.Topology;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import javax.validation.Valid;

import static io.axoniq.axonserver.AxonServerAccessController.CONTEXT_PARAM;
import static io.axoniq.axonserver.AxonServerAccessController.TOKEN_PARAM;

/**
 * @author Marc Gathier
 */
@RestController("CommandRestController")
@RequestMapping("/v1")
public class CommandRestController {

    private final CommandDispatcher commandDispatcher;
    private final CommandRegistrationCache registrationCache;


    public CommandRestController(CommandDispatcher commandDispatcher, CommandRegistrationCache registrationCache) {
        this.commandDispatcher = commandDispatcher;
        this.registrationCache = registrationCache;
    }

    @GetMapping("/components/{component}/commands")
    public Iterable<ComponentCommand> getByComponent(@PathVariable("component") String component, @RequestParam("context") String context){
        return new ComponentItems<>(component, context, new DefaultCommands( registrationCache));
    }

    @GetMapping("commands")
    public List<JsonClientMapping> get() {
        return registrationCache.getAll().entrySet().stream().map(JsonClientMapping::from).collect(Collectors.toList());
    }

    @PostMapping("commands/run")
    @ApiImplicitParams({
            @ApiImplicitParam(name = TOKEN_PARAM, value = "Access Token",
                    required = false, dataType = "string", paramType = "header")
    })
    public Future<CommandResponseJson> execute(@RequestHeader(value = CONTEXT_PARAM, defaultValue = Topology.DEFAULT_CONTEXT, required = false) String context,
                                               @RequestBody @Valid CommandRequestJson command) {
        CompletableFuture<CommandResponseJson> result = new CompletableFuture<>();
        commandDispatcher.dispatch(context, new SerializedCommand(command.asCommand()), r -> result.complete(new CommandResponseJson(r.wrapped())), false);
        return result;
    }

    @GetMapping("commands/queues")
    public List<JsonQueueInfo> queues() {
        return commandDispatcher.getCommandQueues().getSegments().entrySet().stream().map(JsonQueueInfo::from).collect(Collectors.toList());
    }

    @GetMapping("commands/count")
    public int count() {
        return commandDispatcher.commandCount();
    }

    @KeepNames
    public static class JsonClientMapping {
        private String client;
        private String component;
        private String proxy;
        private Set<String> commands;

        public String getClient() {
            return client;
        }

        public String getProxy() {
            return proxy;
        }

        public Set<String> getCommands() {
            return commands;
        }

        public String getComponent() {
            return component;
        }

        static JsonClientMapping from(String component, String client, String proxy) {
            JsonClientMapping jsonCommandMapping = new JsonClientMapping();
            jsonCommandMapping.client = client;
            jsonCommandMapping.component = component;
            jsonCommandMapping.commands = Collections.emptySet();
            jsonCommandMapping.proxy = proxy;
            return jsonCommandMapping;
        }

        static JsonClientMapping from(Map.Entry<CommandHandler, Set<CommandRegistrationCache.RegistrationEntry>> entry) {
            JsonClientMapping jsonCommandMapping = new JsonClientMapping();
            CommandHandler commandHandler = entry.getKey();
            jsonCommandMapping.client = commandHandler.getClient().toString();
            jsonCommandMapping.component = commandHandler.getComponentName();
            jsonCommandMapping.proxy = commandHandler.getMessagingServerName();

            jsonCommandMapping.commands = entry.getValue().stream().map(e -> e.getCommand()).collect(Collectors.toSet());
            return jsonCommandMapping;
        }

    }

    @KeepNames
    private static class JsonQueueInfo {
        private final String client;
        private final int count;

        private JsonQueueInfo(String client, int count) {
            this.client = client;
            this.count = count;
        }

        public String getClient() {
            return client;
        }

        public int getCount() {
            return count;
        }

        static JsonQueueInfo from(Map.Entry<String, ? extends Queue> segment) {
            return new JsonQueueInfo(segment.getKey(), segment.getValue().size());
        }
    }
}
