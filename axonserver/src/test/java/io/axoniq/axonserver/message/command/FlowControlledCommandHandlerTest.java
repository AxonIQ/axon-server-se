/*
 *  Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.command;

import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.grpc.CommandStream;
import io.axoniq.axonserver.grpc.MetaDataValue;
import io.axoniq.axonserver.grpc.ProcessingInstruction;
import io.axoniq.axonserver.grpc.ProcessingKey;
import io.axoniq.axonserver.grpc.SerializedCommand;
import io.axoniq.axonserver.grpc.SerializedCommandProviderInbound;
import io.axoniq.axonserver.grpc.SerializedCommandResponse;
import io.axoniq.axonserver.grpc.command.Command;
import io.axoniq.axonserver.grpc.command.CommandResponse;
import io.axoniq.axonserver.message.ClientStreamIdentification;
import io.axoniq.axonserver.test.FakeStreamObserver;
import org.junit.*;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static io.axoniq.axonserver.test.AssertUtils.assertWithin;
import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Marc Gathier
 * @since
 */
public class FlowControlledCommandHandlerTest {

    private final FakeStreamObserver<SerializedCommandProviderInbound> responseObserver = new FakeStreamObserver<>();
    private final CommandStream commandStream = new CommandStream(responseObserver, 10);
    private final FlowControlledCommandHandler testSubject = new FlowControlledCommandHandler(
                                                                                              new ClientStreamIdentification("context", "streamId"),
                                                                                              "clientId",
                                                                                              "applicationName",
                                                                                              commandStream);

    @Test
    public void dispatch() throws InterruptedException {
        commandStream.addPermits(1);
        testSubject.dispatch(new SerializedCommand(newCommand("1"))).subscribe(r -> System.out.println(r));
        assertWithin(1, TimeUnit.SECONDS, () -> assertEquals(1, responseObserver.values().size()));
        testSubject.dispatch(new SerializedCommand(newCommand("2"))).subscribe(r -> System.out.println(r));
        Thread.sleep(100);
        assertEquals(1, testSubject.waiting());
        assertEquals(1, responseObserver.values().size());
        commandStream.addPermits(1);
        assertWithin(1, TimeUnit.SECONDS, () -> assertEquals(2, responseObserver.values().size()));
        testSubject.commandResponse(newCommandResponse("2"));
        testSubject.commandResponse(newCommandResponse("1"));
    }

    private SerializedCommandResponse newCommandResponse(String s) {
        return new SerializedCommandResponse(CommandResponse.newBuilder()
                                                            .setRequestIdentifier(s)
                                                            .setMessageIdentifier(UUID.randomUUID().toString())
                                                            .build());
    }

    @Test
    public void cancel()  {
        testSubject.dispatch(new SerializedCommand(newCommand("1"))).subscribe(System.out::println);
        testSubject.close();
        assertEquals(0, testSubject.waiting());
        testSubject.dispatch(new SerializedCommand(newCommand("2"))).subscribe(r -> System.out.println(r));
        assertEquals(0, testSubject.waiting());
    }

    @Test
    public void overflowHardLimit() throws InterruptedException {
        for (int i = 0; i < 10; i++) {
            testSubject.dispatch(new SerializedCommand(
                    newCommand("1" + i))).subscribe(System.out::println);
        }
        Command command = Command.newBuilder()
                                 .setMessageIdentifier("3")
                                 .setName("command")
                .addProcessingInstructions(ProcessingInstruction.newBuilder()
                                                   .setKey(ProcessingKey.PRIORITY)
                                                   .setValue(MetaDataValue.newBuilder().setNumberValue(10).build())
                                                                .build())
                                 .build();
        Mono<SerializedCommandResponse> responseMono3 = testSubject.dispatch(new SerializedCommand(command));
        try {
            SerializedCommandResponse response = responseMono3.block(Duration.ofMillis(200));
            fail("Expecting illegal state exception for timeout");
        } catch (IllegalStateException illegalStateException) {

        }
        assertEquals(11, commandStream.waiting());
        command = Command.newBuilder(command).setMessageIdentifier("4").build();
        Mono<SerializedCommandResponse> responseMono4 = testSubject.dispatch(new SerializedCommand(command));
        SerializedCommandResponse response = responseMono4.block(Duration.ofMillis(200));
        assertNotNull(response);
        assertEquals(ErrorCode.COMMAND_DISPATCH_ERROR.getCode(), response.getErrorCode());
        assertEquals(11, commandStream.waiting());
    }

    @Test
    public void overflowSoftLimit() throws InterruptedException {
        for (int i = 0; i < 10; i++) {
            testSubject.dispatch(new SerializedCommand(
                    newCommand("1" + i))).subscribe(System.out::println);
        }
        assertEquals(10, commandStream.waiting());
        Mono<SerializedCommandResponse> responseMono3 = testSubject.dispatch(new SerializedCommand(newCommand("3")));
        SerializedCommandResponse response = responseMono3.block(Duration.ofMillis(200));
        assertNotNull(response);
        assertEquals(ErrorCode.COMMAND_DISPATCH_ERROR.getCode(), response.getErrorCode());
        assertEquals(10, commandStream.waiting());
    }

    private Command newCommand(String id) {
        return Command.newBuilder().setMessageIdentifier(id).setName("command").build();
    }
}