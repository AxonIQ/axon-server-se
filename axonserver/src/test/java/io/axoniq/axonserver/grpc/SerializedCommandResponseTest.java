/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.grpc.command.CommandResponse;
import org.junit.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class SerializedCommandResponseTest {
    private SerializedCommandResponse testSubject;

    @Test
    public void testSerializeDeserializeConfirmation() throws IOException {
        CommandResponse commandResponse = CommandResponse.newBuilder()
                                                         .setRequestIdentifier("12345")
                                                         .build();
        testSubject = new SerializedCommandResponse(commandResponse);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        testSubject.writeTo(outputStream);

        SerializedCommandResponse parsed = (SerializedCommandResponse) SerializedCommandResponse.getDefaultInstance().getParserForType()
                                                                                                                     .parseFrom(outputStream.toByteArray());
        assertEquals("12345", parsed.getRequestIdentifier());
    }

}