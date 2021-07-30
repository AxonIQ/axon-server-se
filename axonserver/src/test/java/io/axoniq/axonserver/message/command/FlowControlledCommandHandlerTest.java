/*
 *  Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.command;

import io.axoniq.axonserver.grpc.CommandStream;
import io.axoniq.axonserver.grpc.SerializedCommand;
import io.axoniq.axonserver.grpc.SerializedCommandProviderInbound;
import io.axoniq.axonserver.grpc.SerializedCommandResponse;
import io.axoniq.axonserver.grpc.command.Command;
import io.axoniq.axonserver.grpc.command.CommandResponse;
import io.axoniq.axonserver.message.ClientStreamIdentification;
import io.axoniq.axonserver.test.FakeStreamObserver;
import org.junit.*;

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
    private final CommandStream commandStream = new CommandStream(responseObserver);
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
        testSubject.dispatch(new SerializedCommand(newCommand("1")));
        testSubject.close();
        assertEquals(0, testSubject.waiting());
        testSubject.dispatch(new SerializedCommand(newCommand("2")));
        assertEquals(0, testSubject.waiting());
    }

    private Command newCommand(String id) {
        return Command.newBuilder().setMessageIdentifier(id).setName("command").build();
    }
}