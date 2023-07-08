/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.transport.grpc.command;

import io.axoniq.axonserver.ProcessingInstructionHelper;
import io.axoniq.axonserver.api.Authentication;
import io.axoniq.axonserver.commandprocessing.spi.Command;
import io.axoniq.axonserver.commandprocessing.spi.Metadata;
import io.axoniq.axonserver.commandprocessing.spi.Payload;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static io.axoniq.axonserver.ProcessingInstructionHelper.routingKey;


public class GrpcCommand implements Command {

    private final io.axoniq.axonserver.grpc.command.Command wrapped;
    private final Payload payload;
    private final Metadata metadata;
    private final String context;

    public GrpcCommand(io.axoniq.axonserver.grpc.command.Command wrapped, String context,
                       Authentication authentication) {
        this(wrapped, context, Collections.emptyMap(), authentication);
    }

    public GrpcCommand(io.axoniq.axonserver.grpc.command.Command wrapped, String context,
                       Map<String, Serializable> internalMetadata,
                       Authentication authentication) {
        this.wrapped = wrapped;
        this.context = context;
        this.payload = new GrpcPayload(wrapped.getPayload());
        Map<String, Serializable> requestMetadata = new HashMap<>(internalMetadata);
        requestMetadata.put(ROUTING_KEY, routingKey(wrapped.getProcessingInstructionsList(),
                                                    wrapped.getMessageIdentifier()));
        requestMetadata.put(PRIORITY, ProcessingInstructionHelper.priority(wrapped.getProcessingInstructionsList()));

        if (authentication != null) {
            requestMetadata.put(PRINCIPAL, authentication);
        }
        requestMetadata.put(CLIENT_ID, wrapped.getClientId());
        requestMetadata.put(COMPONENT_NAME, wrapped().getComponentName());
        metadata = new GrpcMetadata(wrapped.getMetaDataMap(), requestMetadata);
    }

    @Override
    public String id() {
        return wrapped.getMessageIdentifier();
    }

    @Override
    public String commandName() {
        return wrapped.getName();
    }

    @Override
    public String context() {
        return context;
    }

    @Override
    public Payload payload() {
        return payload;
    }

    @Override
    public Metadata metadata() {
        return metadata;
    }

    public io.axoniq.axonserver.grpc.command.Command wrapped() {
        return wrapped;
    }
}
