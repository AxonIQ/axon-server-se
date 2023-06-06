/*
 *  Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.command;

import io.axoniq.axonserver.api.Authentication;
import io.axoniq.axonserver.commandprocesing.imp.DefaultCommandRequestProcessor;
import io.axoniq.axonserver.commandprocesing.imp.InMemoryCommandHandlerRegistry;
import io.axoniq.axonserver.commandprocessing.spi.Command;
import io.axoniq.axonserver.commandprocessing.spi.CommandHandler;
import io.axoniq.axonserver.commandprocessing.spi.CommandHandlerSubscription;
import io.axoniq.axonserver.commandprocessing.spi.CommandResult;
import io.axoniq.axonserver.grpc.MetaDataValue;
import io.axoniq.axonserver.grpc.SerializedCommand;
import io.axoniq.axonserver.grpc.SerializedCommandResponse;
import io.axoniq.axonserver.grpc.command.CommandResponse;
import io.axoniq.axonserver.interceptor.CommandInterceptors;
import io.axoniq.axonserver.plugin.ExecutionContext;
import io.axoniq.axonserver.transport.grpc.command.GrpcCommandResult;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Mono;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CommandPluginHandlerTest {

    public static final String SIMPLE_COMMAND = "command";
    public static final String FAILING_COMMAND = "failingCommand";
    private final List<BiFunction<SerializedCommand, ExecutionContext, SerializedCommand>> commandInterceptors = new ArrayList<>();
    private final List<BiFunction<SerializedCommandResponse, ExecutionContext, SerializedCommandResponse>> responseInterceptors = new ArrayList<>();
    private boolean commandInterceptorInvoked;
    private boolean responseInterceptorInvoked;
    private final DefaultCommandRequestProcessor commandRequestProcessor = new DefaultCommandRequestProcessor(new InMemoryCommandHandlerRegistry()
    );
    private final CommandPluginHandler testSubject = new CommandPluginHandler(commandRequestProcessor,
                                                                              new CommandInterceptors() {
                                                                                  @Override
                                                                                  public SerializedCommand commandRequest(
                                                                                          SerializedCommand serializedCommand,
                                                                                          ExecutionContext executionContext) {
                                                                                      commandInterceptorInvoked = true;
                                                                                      return commandInterceptors.stream()
                                                                                                                .reduce(serializedCommand,
                                                                                                                        (prev, interceptor) -> interceptor.apply(
                                                                                                                                prev,
                                                                                                                                executionContext),
                                                                                                                        (old, next) -> next);
                                                                                  }

                                                                                  @Override
                                                                                  public SerializedCommandResponse commandResponse(
                                                                                          SerializedCommandResponse serializedResponse,
                                                                                          ExecutionContext executionContext) {
                                                                                      responseInterceptorInvoked = true;
                                                                                      return responseInterceptors.stream()
                                                                                                                 .reduce(serializedResponse,
                                                                                                                         (prev, interceptor) -> interceptor.apply(
                                                                                                                                 prev,
                                                                                                                                 executionContext),
                                                                                                                         (old, next) -> next);
                                                                                  }

                                                                                  @Override
                                                                                  public boolean noRequestInterceptors(
                                                                                          String context) {
                                                                                      return commandInterceptors.isEmpty();
                                                                                  }

                                                                                  @Override
                                                                                  public boolean noResponseInterceptors(
                                                                                          String context) {
                                                                                      return responseInterceptors.isEmpty();
                                                                                  }
                                                                              });

    @Before
    public void registerCommandHandler() {
        commandRequestProcessor.register(new CommandHandlerSubscription() {
            @Override
            public CommandHandler commandHandler() {
                return CommandUtils.getCommandHandler(SIMPLE_COMMAND);
            }

            @Override
            public Mono<CommandResult> dispatch(Command command) {
                return Mono.fromSupplier(() -> {
                    CommandResponse.Builder responseBuilder = CommandResponse.newBuilder()
                                                                             .setMessageIdentifier(UUID.randomUUID()
                                                                                                       .toString())
                                                                             .setRequestIdentifier(command.id());
                    command.metadata().metadataKeys().forEach(k -> {
                        Optional<Serializable> value = command.metadata().metadataValue(k);
                        value.ifPresent(v -> {
                            responseBuilder.putMetaData(k, MetaDataValue.newBuilder()
                                                                        .setTextValue(String.valueOf(v))
                                                                        .build());
                        });
                    });
                    return new GrpcCommandResult(responseBuilder.build());
                });
            }
        }).block();
        commandRequestProcessor.register(new CommandHandlerSubscription() {
            @Override
            public CommandHandler commandHandler() {
                return CommandUtils.getCommandHandler(FAILING_COMMAND);
            }

            @Override
            public Mono<CommandResult> dispatch(Command command) {
                return Mono.error(new RuntimeException());
            }
        }).block();
    }

    @Test
    public void noCommandInterceptors() {
        commandRequestProcessor.dispatch(CommandUtils.getCommandRequest(SIMPLE_COMMAND)).block(Duration.ofSeconds(1));
        assertFalse(this.commandInterceptorInvoked);
        assertFalse(this.responseInterceptorInvoked);
    }

    @Test
    public void onlyResponseInterceptor() {
        responseInterceptors.add((r, c) -> r);
        commandRequestProcessor.dispatch(CommandUtils.getCommandRequest(SIMPLE_COMMAND)).block(Duration.ofSeconds(1));
        assertFalse(this.commandInterceptorInvoked);
        assertTrue(this.responseInterceptorInvoked);
    }

    @Test
    public void onlyRequestInterceptor() {
        commandInterceptors.add((r, c) -> r);
        commandRequestProcessor.dispatch(CommandUtils.getCommandRequest(SIMPLE_COMMAND)).block(Duration.ofSeconds(1));
        assertTrue(this.commandInterceptorInvoked);
        assertFalse(this.responseInterceptorInvoked);
    }

    @Test
    public void useAuthentication() {
        commandInterceptors.add((r, c) -> {
            io.axoniq.axonserver.grpc.command.Command grpcCommand = r.wrapped();
            return new SerializedCommand(grpcCommand.toBuilder().putMetaData("issuedBy", MetaDataValue.newBuilder()
                                                                                                      .setTextValue(c.principal())
                                                                                                      .build())
                                                    .build());
        });
        Map<String, Serializable> metadata = new HashMap<>();
        metadata.put(Command.PRINCIPAL, new Authentication() {
            @Override
            public boolean hasAnyRole(@NotNull String context) {
                return true;
            }

            @Override
            public boolean isLocallyManaged() {
                return true;
            }

            @NotNull
            @Override
            public String username() {
                return "user";
            }

            @Override
            public boolean hasRole(@NotNull String role, @NotNull String context) {
                return false;
            }

            @Override
            public boolean application() {
                return false;
            }
        });
        CommandResult response = commandRequestProcessor.dispatch(CommandUtils.getCommandRequest(SIMPLE_COMMAND,
                                                                                                 CommandUtils.metadata(
                                                                                                         metadata)))
                                                        .block(Duration.ofSeconds(1));
        Optional<Serializable> optionalIssuedBy = response.metadata().metadataValue("issuedBy");
        assertTrue(optionalIssuedBy.isPresent());
        assertEquals("user", optionalIssuedBy.get());
        assertTrue(this.commandInterceptorInvoked);
    }

    @Test
    public void failedCommand() {
        AtomicBoolean compensated = new AtomicBoolean();
        commandInterceptors.add((r, c) -> {
            c.onFailure((t, context) -> compensated.set(true));
            return r;
        });
        try {
            commandRequestProcessor.dispatch(CommandUtils.getCommandRequest(FAILING_COMMAND))
                                   .block(Duration.ofSeconds(1));
        } catch (RuntimeException re) {
            // expected
        }
        assertTrue(this.commandInterceptorInvoked);
        assertFalse(this.responseInterceptorInvoked);
        assertTrue(compensated.get());
    }

    @Test
    public void failedCommandInterceptor() {
        AtomicBoolean compensated = new AtomicBoolean();
        commandInterceptors.add((r, c) -> {
            c.onFailure((t, context) -> compensated.set(true));
            return r;
        });
        commandInterceptors.add((r, c) -> {
            throw new RuntimeException();
        });
        try {
            commandRequestProcessor.dispatch(CommandUtils.getCommandRequest(SIMPLE_COMMAND))
                                   .block(Duration.ofSeconds(1));
        } catch (RuntimeException re) {
            // expected
        }
        assertTrue(this.commandInterceptorInvoked);
        assertFalse(this.responseInterceptorInvoked);
        assertTrue(compensated.get());
    }
}