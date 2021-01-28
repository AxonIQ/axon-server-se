/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.component.ComponentItems;
import io.axoniq.axonserver.component.command.ComponentCommand;
import io.axoniq.axonserver.component.command.DefaultCommands;
import io.axoniq.axonserver.config.GrpcContextAuthenticationProvider;
import io.axoniq.axonserver.grpc.SerializedCommand;
import io.axoniq.axonserver.logging.AuditLog;
import io.axoniq.axonserver.message.command.CommandDispatcher;
import io.axoniq.axonserver.message.command.CommandHandler;
import io.axoniq.axonserver.message.command.CommandRegistrationCache;
import io.axoniq.axonserver.rest.json.CommandRequestJson;
import io.axoniq.axonserver.rest.json.CommandResponseJson;
import io.axoniq.axonserver.topology.Topology;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import org.slf4j.Logger;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import springfox.documentation.annotations.ApiIgnore;

import java.security.Principal;
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
import static io.axoniq.axonserver.util.ObjectUtils.getOrDefault;
import static io.axoniq.axonserver.util.StringUtils.sanitize;

/**
 * REST controller to retrieve information about subscribed commands and to dispatch commands.
 *
 * @author Marc Gathier
 * @since 4.0
 */
@RestController("CommandRestController")
@RequestMapping("/v1")
public class CommandRestController {

    private static final Logger auditLog = AuditLog.getLogger();

    private final CommandDispatcher commandDispatcher;
    private final CommandRegistrationCache registrationCache;


    public CommandRestController(CommandDispatcher commandDispatcher, CommandRegistrationCache registrationCache) {
        this.commandDispatcher = commandDispatcher;
        this.registrationCache = registrationCache;
    }

    @GetMapping("/components/{component}/commands")
    public Iterable<ComponentCommand> getByComponent(@PathVariable("component") String component,
                                                     @RequestParam("context") String context,
                                                     @ApiIgnore Principal principal) {
        auditLog.info(
                "[{}] Request to list all Commands for which component \"{}\" in context \"{}\" has a registered handler.",
                AuditLog.username(principal),
                component,
                sanitize(context));

        return new ComponentItems<>(component, context, new DefaultCommands(registrationCache));
    }

    @GetMapping("commands")
    public List<JsonClientMapping> get(@ApiIgnore Principal principal) {
        auditLog.info("[{}] Request to list all Commands which have a registered handler.",
                      AuditLog.username(principal));

        return registrationCache.getAll().entrySet().stream().map(JsonClientMapping::from).collect(Collectors.toList());
    }

    @PostMapping("commands/run")
    @ApiImplicitParams({
            @ApiImplicitParam(name = TOKEN_PARAM, value = "Access Token",
                    required = false, dataType = "string", paramType = "header")
    })
    public Future<CommandResponseJson> execute(
            @RequestHeader(value = CONTEXT_PARAM, defaultValue = Topology.DEFAULT_CONTEXT, required = false) String context,
            @RequestBody @Valid CommandRequestJson command,
            @ApiIgnore Authentication principal) {
        auditLog.info("[{}] Request to dispatch a \"{}\" Command.", AuditLog.username(principal), command.getName());

        CompletableFuture<CommandResponseJson> result = new CompletableFuture<>();
        commandDispatcher.dispatch(context,
                                   getOrDefault(principal, GrpcContextAuthenticationProvider.DEFAULT_PRINCIPAL),
                                   new SerializedCommand(command.asCommand()),
                                   r -> result.complete(new CommandResponseJson(r.wrapped())));
        return result;
    }

    @GetMapping("commands/queues")
    public List<JsonQueueInfo> queues(@ApiIgnore Principal principal) {
        auditLog.info("[{}] Request to list all CommandQueues.", AuditLog.username(principal));

        return commandDispatcher.getCommandQueues().getSegments().entrySet().stream().map(JsonQueueInfo::from).collect(
                Collectors.toList());
    }

    @GetMapping("commands/count")
    public int count(@ApiIgnore Principal principal) {
        if (auditLog.isDebugEnabled()) {
            auditLog.debug("[{}] Request for the active command count.", AuditLog.username(principal));
        }
        return commandDispatcher.activeCommandCount();
    }

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
            jsonCommandMapping.client = commandHandler.getClientStreamIdentification().toString();
            jsonCommandMapping.component = commandHandler.getComponentName();
            jsonCommandMapping.proxy = commandHandler.getMessagingServerName();

            jsonCommandMapping.commands = entry.getValue().stream().map(e -> e.getCommand()).collect(Collectors.toSet());
            return jsonCommandMapping;
        }

    }

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
