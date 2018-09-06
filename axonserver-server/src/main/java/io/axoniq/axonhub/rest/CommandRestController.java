package io.axoniq.axonhub.rest;

import io.axoniq.axonhub.KeepNames;
import io.axoniq.axonhub.component.ComponentItems;
import io.axoniq.axonhub.component.command.ComponentCommand;
import io.axoniq.axonhub.component.command.DefaultCommands;
import io.axoniq.axonhub.grpc.internal.ProxyCommandHandler;
import io.axoniq.axonhub.message.command.CommandDispatcher;
import io.axoniq.axonhub.message.command.CommandHandler;
import io.axoniq.axonhub.message.command.CommandRegistrationCache;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Author: marc
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
            jsonCommandMapping.client = commandHandler.getClient();
            jsonCommandMapping.component = commandHandler.getComponentName();
            if( commandHandler instanceof ProxyCommandHandler) {
                jsonCommandMapping.proxy = ((ProxyCommandHandler) commandHandler).getMessagingServerName();
            }

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
