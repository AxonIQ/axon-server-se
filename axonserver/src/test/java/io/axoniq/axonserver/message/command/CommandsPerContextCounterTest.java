/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.command;

import io.axoniq.axonserver.commandprocesing.imp.DefaultCommandRequestProcessor;
import io.axoniq.axonserver.commandprocesing.imp.InMemoryCommandHandlerRegistry;
import io.axoniq.axonserver.commandprocessing.spi.Command;
import io.axoniq.axonserver.commandprocessing.spi.CommandHandler;
import io.axoniq.axonserver.commandprocessing.spi.CommandHandlerSubscription;
import io.axoniq.axonserver.commandprocessing.spi.CommandResult;
import io.axoniq.axonserver.grpc.MetaDataValue;
import io.axoniq.axonserver.grpc.command.CommandResponse;
import io.axoniq.axonserver.transport.grpc.command.GrpcCommandResult;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.Serializable;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.axoniq.axonserver.test.AssertUtils.assertWithin;
import static org.junit.Assert.assertEquals;

public class CommandsPerContextCounterTest {

    public static final String SIMPLE_COMMAND = "command";
    public static final String FAILING_COMMAND = "failingCommand";
    public static final String WAITING_COMMAND = "waitingCommand";
    private final DefaultCommandRequestProcessor commandRequestProcessor =
            new DefaultCommandRequestProcessor(new InMemoryCommandHandlerRegistry());

    private final MeterRegistry meterRegistry = new SimpleMeterRegistry();

    private final CountDownLatch countDownLatch = new CountDownLatch(1);
    private final CommandsPerContextCounter testSubject = new CommandsPerContextCounter(commandRequestProcessor,
                                                                                        meterRegistry);

    @Before
    public void registerCommandHandlers() {
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
                return CommandUtils.getCommandHandler(WAITING_COMMAND);
            }

            @Override
            public Mono<CommandResult> dispatch(Command command) {
                return Mono.fromSupplier(() -> {
                    CommandResponse.Builder responseBuilder = CommandResponse.newBuilder()
                                                                             .setMessageIdentifier(UUID.randomUUID()
                                                                                                       .toString())
                                                                             .setRequestIdentifier(command.id());
                    try {
                        if (!countDownLatch.await(1, TimeUnit.SECONDS)) {
                            throw new RuntimeException("Latch not completed");
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
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
    public void activeCommandsZeroAfterCommand() {
        commandRequestProcessor.dispatch(CommandUtils.getCommandRequest(SIMPLE_COMMAND)).block(Duration.ofSeconds(1));
        assertEquals(0, testSubject.get(CommandUtils.CONTEXT));
    }

    @Test
    public void activeCommandsSetDuringCommand() throws ExecutionException, InterruptedException, TimeoutException {
        CompletableFuture<Void> done = new CompletableFuture<>();
        commandRequestProcessor.dispatch(CommandUtils.getCommandRequest(WAITING_COMMAND))
                               .subscribeOn(Schedulers.single())
                               .subscribe(r -> {
                                          },
                                          done::completeExceptionally,
                                          () -> done.complete(null));
        assertWithin(100, TimeUnit.MILLISECONDS,
                     () -> assertEquals(1, testSubject.get(CommandUtils.CONTEXT)));
        countDownLatch.countDown();
        done.get(1, TimeUnit.SECONDS);
        assertEquals(0, testSubject.get(CommandUtils.CONTEXT));
    }

    @Test
    public void activeCommandsZeroAfterFailure() {
        commandRequestProcessor.dispatch(CommandUtils.getCommandRequest(SIMPLE_COMMAND)).block(Duration.ofSeconds(1));
        assertEquals(0, testSubject.get(CommandUtils.CONTEXT));
    }
}