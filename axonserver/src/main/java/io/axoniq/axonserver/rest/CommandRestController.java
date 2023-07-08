/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.rest;

import com.google.protobuf.ByteString;
import io.axoniq.axonserver.commandprocessing.spi.Command;
import io.axoniq.axonserver.commandprocessing.spi.CommandRequestProcessor;
import io.axoniq.axonserver.commandprocessing.spi.Metadata;
import io.axoniq.axonserver.commandprocessing.spi.NoHandlerFoundException;
import io.axoniq.axonserver.commandprocessing.spi.Payload;
import io.axoniq.axonserver.component.ComponentItems;
import io.axoniq.axonserver.component.command.ComponentCommand;
import io.axoniq.axonserver.component.command.DefaultCommands;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.logging.AuditLog;
import io.axoniq.axonserver.message.command.CommandSubscriptionCache;
import io.axoniq.axonserver.rest.json.CommandRequestJson;
import io.axoniq.axonserver.rest.json.CommandResponseJson;
import io.axoniq.axonserver.topology.Topology;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.Parameters;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.Serializable;
import java.security.Principal;
import java.util.Optional;
import javax.validation.Valid;

import static io.axoniq.axonserver.AxonServerAccessController.CONTEXT_PARAM;
import static io.axoniq.axonserver.AxonServerAccessController.TOKEN_PARAM;
import static io.axoniq.axonserver.util.StringUtils.sanitize;

/**
 * REST controller to retrieve information about subscribed commands and to dispatch commands.
 *
 * @author Marc Gathier
 * @author Stefan Dragisic
 *
 * @since 4.0
 */
@RestController("CommandRestController")
@RequestMapping("/v1")
public class CommandRestController {

    private static final Logger auditLog = AuditLog.getLogger();
    private final Logger logger = LoggerFactory.getLogger(CommandRestController.class);

    private final CommandRequestProcessor commandRequestProcessor;
    private final CommandSubscriptionCache commandSubscriptionCache;


    public CommandRestController(CommandRequestProcessor commandRequestProcessor,
                                 CommandSubscriptionCache commandSubscriptionCache) {
        this.commandRequestProcessor = commandRequestProcessor;
        this.commandSubscriptionCache = commandSubscriptionCache;
    }

    @GetMapping("/components/{component}/commands")
    public Iterable<ComponentCommand> getByComponent(@PathVariable("component") String component,
                                                     @RequestParam("context") String context,
                                                     @Parameter(hidden = true) Principal principal) {
        auditLog.info(
                "[{}] Request to list all Commands for which component \"{}\" in context \"{}\" has a registered handler.",
                AuditLog.username(principal),
                component,
                sanitize(context));

        return new ComponentItems<>(component, context, new DefaultCommands(commandSubscriptionCache, component));
    }

    @PostMapping("commands/run")
    @Parameters({
            @Parameter(name = TOKEN_PARAM, description = "Access Token", in = ParameterIn.HEADER)
    })
    public Mono<CommandResponseJson> execute(
            @RequestHeader(value = CONTEXT_PARAM, defaultValue = Topology.DEFAULT_CONTEXT, required = false) String context,
            @RequestBody @Valid CommandRequestJson command,
            @Parameter(hidden = true) Authentication principal) {
        auditLog.info("[{}] Request to dispatch a \"{}\" Command.", AuditLog.username(principal), command.getName());
        return commandRequestProcessor.dispatch(new WrappedJsonCommand(command, context))
                                      .map(CommandResponseJson::new)
                                      .onErrorMap(NoHandlerFoundException.class,
                                                  t -> new MessagingPlatformException(ErrorCode.NO_HANDLER_FOR_COMMAND,
                                                                                      "No handler found for "
                                                                                              + command.getName()))
                                      .onErrorResume(e -> {
                                          logger.error("Error occurred while exciting command: ", e);
                                          return Mono.just(new CommandResponseJson(command.getMessageIdentifier(), e));
                                      });
    }

    private static class WrappedJsonCommand implements Command {

        private final CommandRequestJson command;
        private final String context;

        public WrappedJsonCommand(CommandRequestJson commandRequestJson, String context) {
            command = commandRequestJson;
            this.context = context;
        }


        @Override
        public String id() {
            return command.getMessageIdentifier();
        }

        @Override
        public String commandName() {
            return command.getName();
        }

        @Override
        public String context() {
            return context;
        }

        @Override
        public Payload payload() {
            return new Payload() {
                @Override
                public String type() {
                    return command.getPayload().getType();
                }

                @Override
                public String contentType() {
                    return null;
                }

                @Override
                public Flux<Byte> data() {
                    return Flux.fromIterable(ByteString.copyFromUtf8(command.getPayload().getData()));
                }
            };
        }

        @Override
        public Metadata metadata() {
            return new Metadata() {
                @Override
                public Iterable<String> metadataKeys() {
                    return command.getMetaData().keySet();
                }

                @Override
                public <R extends Serializable> Optional<R> metadataValue(String metadataKey) {
                    return Optional.ofNullable((R) command.getMetaData().get(metadataKey));
                }
            };
        }
    };
}
