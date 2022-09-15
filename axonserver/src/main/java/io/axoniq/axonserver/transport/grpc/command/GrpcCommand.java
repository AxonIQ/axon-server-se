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
import io.axoniq.axonserver.grpc.MetaDataValue;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


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
        Map<String, MetaDataValue> requestMetadata = new HashMap<>(wrapped.getMetaDataMap());
        requestMetadata.put(ROUTING_KEY, MetaDataValue.newBuilder()
                                                      .setTextValue(ProcessingInstructionHelper.routingKey(wrapped.getProcessingInstructionsList(),
                                                                                                           "1234"))
                                                      .build());
        requestMetadata.put(PRIORITY, MetaDataValue.newBuilder()
                                                   .setNumberValue(ProcessingInstructionHelper.priority(wrapped.getProcessingInstructionsList()))
                                                   .build());
        if (authentication != null) {
            requestMetadata.put(PRINCIPAL, MetaDataValue.newBuilder()
                                                        .setTextValue(authentication.username())
                                                        .build());
        }
        requestMetadata.put(CLIENT_ID, MetaDataValue.newBuilder()
                                                    .setTextValue(wrapped().getClientId())
                                                    .build());
        requestMetadata.put(COMPONENT_NAME, MetaDataValue.newBuilder()
                                                         .setTextValue(wrapped().getComponentName())
                                                         .build());
        metadata = new GrpcMetadata(requestMetadata, internalMetadata);
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
