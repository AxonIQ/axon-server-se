/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.transport.grpc.command;

import io.axoniq.axonserver.commandprocessing.spi.ResultPayload;
import io.axoniq.axonserver.grpc.command.CommandResponse;
import reactor.core.publisher.Flux;

public class GrpcResultPayload implements ResultPayload {

    private final GrpcPayload payload;
    private final CommandResponse commandResponse;

    public GrpcResultPayload(CommandResponse commandResponse) {
        this.payload = commandResponse.hasPayload() ? new GrpcPayload(commandResponse.getPayload()) : null;
        this.commandResponse = commandResponse;
    }


    @Override
    public String type() {
        return payload != null ? payload.type() : null;
    }

    @Override
    public String contentType() {
        return null;
    }

    @Override
    public Flux<Byte> data() {
        return payload != null ? payload.data() : Flux.empty();
    }

    @Override
    public boolean error() {
        return commandResponse.hasErrorMessage();
    }
}
