/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.transport.grpc.command;

import io.axoniq.axonserver.commandprocessing.spi.Payload;
import io.axoniq.axonserver.grpc.SerializedObject;
import reactor.core.publisher.Flux;

public class GrpcPayload implements Payload {

    private final SerializedObject payload;

    public GrpcPayload(SerializedObject payload) {
        this.payload = payload;
    }

    @Override
    public String type() {
        return payload.getType();
    }

    @Override
    public String contentType() {
        return null;
    }

    @Override
    public Flux<Byte> data() {
        return Flux.fromIterable(payload.getData());
    }
}
