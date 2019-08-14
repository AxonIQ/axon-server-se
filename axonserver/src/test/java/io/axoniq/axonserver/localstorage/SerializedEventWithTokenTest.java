/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage;

import com.google.protobuf.ByteString;
import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.EventWithToken;
import org.junit.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

import static org.junit.Assert.*;

public class SerializedEventWithTokenTest {

    @Test
    public void testEncodedDataIdenticalToGrpcOwnDecoding() throws IOException {
        Event event = Event.newBuilder()
                           .setAggregateType("test")
                                             .setPayload(SerializedObject.newBuilder()
                                                                         .setType("test2")
                                                                         .setRevision("Rev")
                                                                         .setData(ByteString.copyFromUtf8("Mock"))
                                                                         .build())
                           .setMessageIdentifier(UUID.randomUUID().toString())
                           .setAggregateSequenceNumber(Long.MAX_VALUE)
                           .setAggregateIdentifier(UUID.randomUUID().toString())
                           .setTimestamp(Long.MAX_VALUE)
                           .build();
        EventWithToken eventWithToken = EventWithToken.newBuilder()
                                                      .setToken(Long.MAX_VALUE)
                                                      .setEvent(event).build();

        SerializedEventWithToken testSubject = new SerializedEventWithToken(eventWithToken.getToken(), new SerializedEvent(event.toByteArray()));


        byte[] actual = toByteArray(testSubject.asInputStream());
        assertArrayEquals(eventWithToken.toByteArray(), actual);
    }

    private byte[] toByteArray(InputStream asInputStream) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        byte[] bytes = new byte[512];
        int r;
        while ((r = asInputStream.read(bytes)) > 0) {
            bos.write(bytes, 0, r);
        }
        return bos.toByteArray();
    }
}
