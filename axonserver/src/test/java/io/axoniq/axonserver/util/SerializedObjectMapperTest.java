/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.util;

import com.google.protobuf.ByteString;
import io.axoniq.axonserver.grpc.SerializedObject;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SerializedObjectMapperTest {

    public static final String THIS_IS_THE_SAMPLE_DATA = "<this is the sample data>\n\r\t";

    @Test
    public void map() {
        SerializedObject serializedObject = SerializedObject.newBuilder()
                                                            .setType("my.type")
                                                            .setData(ByteString.copyFromUtf8(THIS_IS_THE_SAMPLE_DATA))
                                                            .build();
        assertEquals(THIS_IS_THE_SAMPLE_DATA, SerializedObjectMapper.map(serializedObject));
    }


    @Test
    public void mapBinary() {
        SerializedObject serializedObject = SerializedObject.newBuilder()
                                                            .setType("my.type")
                                                            .setData(ByteString.copyFromUtf8(THIS_IS_THE_SAMPLE_DATA))
                                                            .build();

        SerializedObject serializedObjectWithBinaryContent = SerializedObject.newBuilder()
                                                                             .setType("my.type")
                                                                             .setData(ByteString.copyFrom(
                                                                                     serializedObject.toByteArray()))
                                                                             .build();
        assertEquals("<binary>", SerializedObjectMapper.map(serializedObjectWithBinaryContent));
    }
}