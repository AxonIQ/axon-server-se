/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.util;

import io.axoniq.axonserver.grpc.SerializedObject;

public class SerializedObjectMapper {

    public static String map(SerializedObject serializedObject) {
        String asString = serializedObject.getData().toStringUtf8();
        // temporary replace tabs and new lines with space
        String printable = asString.replaceAll("\\p{Space}", " ");
        // remove all non-printable characters
        printable = printable.replaceAll("\\P{Print}", "");
        return asString.length() > printable.length() ? "<binary>" : asString;
    }
}
