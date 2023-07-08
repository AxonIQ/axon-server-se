/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.transport.grpc.command;

import io.axoniq.axonserver.commandprocessing.spi.CommandResult;
import io.axoniq.axonserver.commandprocessing.spi.Metadata;
import io.axoniq.axonserver.commandprocessing.spi.ResultPayload;
import io.axoniq.axonserver.grpc.command.CommandResponse;

public class GrpcCommandResult implements CommandResult {

    private final CommandResponse commandResponse;

    public GrpcCommandResult(CommandResponse commandResponse) {
        this.commandResponse = commandResponse;
    }

    @Override
    public String id() {
        return commandResponse.getMessageIdentifier();
    }

    @Override
    public String commandId() {
        return commandResponse.getRequestIdentifier();
    }

    @Override
    public ResultPayload payload() {
        return new GrpcResultPayload(commandResponse);
    }

    @Override
    public Metadata metadata() {
        return new GrpcMetadata(commandResponse.getMetaDataMap());
    }

    public CommandResponse wrapped() {
        return commandResponse;
    }
}
