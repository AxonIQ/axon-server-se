/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.grpc;

import com.google.protobuf.Message;
import io.axoniq.axonserver.grpc.command.Command;
import org.junit.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import static org.junit.Assert.*;

/**
 * Author: marc
 */
public class SerializedCommandProviderInboundTest {

    private SerializedCommandProviderInbound testSubject;

    @Test
    public void testSerializeDeserializeConfirmation() throws IOException {
        Confirmation conformation = Confirmation.newBuilder()
                                                .setSuccess(true)
                                                .setMessageId("12345")
                                                .build();
        testSubject = new SerializedCommandProviderInbound(null, conformation);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        testSubject.writeTo(outputStream);

        SerializedCommandProviderInbound parsed = (SerializedCommandProviderInbound) SerializedCommandProviderInbound.getDefaultInstance().getParserForType()
                                                         .parseFrom(outputStream.toByteArray());
        assertNotNull(parsed.getConfirmation());
        assertEquals("12345", parsed.getConfirmation().getMessageId());
    }

    @Test
    public void testSerializeDeserializeCommand() throws IOException {
        SerializedCommand serializedCommand = new SerializedCommand(Command.newBuilder().setName("COMMAND").build());
        testSubject = new SerializedCommandProviderInbound(serializedCommand, null);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        testSubject.writeTo(outputStream);

        SerializedCommandProviderInbound parsed = (SerializedCommandProviderInbound) SerializedCommandProviderInbound.getDefaultInstance().getParserForType()
                                                                                                                     .parseFrom(outputStream.toByteArray());
        assertNotNull(parsed.getSerializedCommand());
        assertEquals("COMMAND", parsed.getSerializedCommand().getName());
    }

}