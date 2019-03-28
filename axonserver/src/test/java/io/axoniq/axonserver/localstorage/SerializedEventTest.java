/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage;

import io.axoniq.axonserver.grpc.MetaDataValue;
import io.axoniq.axonserver.grpc.event.Event;
import org.junit.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class SerializedEventTest {

    private SerializedEvent testSubject;
    private Event wrapped;

    @Before
    public void setUp() {
        wrapped = Event.newBuilder()
                       .setAggregateIdentifier("aggId")
                       .setAggregateSequenceNumber(10)
                       .putMetaData("sample", MetaDataValue.newBuilder().setTextValue("sampleText").build())
                       .putMetaData("sampleInt", MetaDataValue.newBuilder().setNumberValue(10).build())
                       .build();
        testSubject = new SerializedEvent(wrapped.toByteArray());
    }

    @Test
    public void asInputStream() throws IOException {
        InputStream inputStream=testSubject.asInputStream();
        byte[] buffer = new byte[4096];
        int r = inputStream.read(buffer);
        buffer = Arrays.copyOf(buffer, r);
        byte[] wrappedBytes = wrapped.toByteArray();
        assertEquals( r, wrappedBytes.length);
        assertArrayEquals(buffer, wrappedBytes);
    }

    @Test
    public void asEvent() {
        assertEquals("aggId", testSubject.asEvent().getAggregateIdentifier());
    }

    @Test
    public void size() {
        assertEquals(wrapped.getSerializedSize(), testSubject.size());
    }

    @Test
    public void serializedData() {
        assertArrayEquals(wrapped.toByteArray(), testSubject.serializedData());
    }

    @Test
    public void getAggregateSequenceNumber() {
        assertEquals(10, testSubject.getAggregateSequenceNumber());
    }

    @Test
    public void getMetaData() {
        assertEquals(10L, testSubject.getMetaData().get("sampleInt"));
        assertEquals("sampleText", testSubject.getMetaData().get("sample"));
    }

    @Test
    public void validateEquals() {
        SerializedEvent other = new SerializedEvent(wrapped);
        assertEquals(other, testSubject);
        assertEquals(other.hashCode(), testSubject.hashCode());
    }
}