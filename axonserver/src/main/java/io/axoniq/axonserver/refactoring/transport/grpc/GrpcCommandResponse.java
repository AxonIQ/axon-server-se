/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.refactoring.transport.grpc;

import io.axoniq.axonserver.refactoring.messaging.api.Error;
import io.axoniq.axonserver.refactoring.messaging.api.Message;
import io.axoniq.axonserver.refactoring.messaging.api.SerializedObject;
import io.axoniq.axonserver.refactoring.messaging.command.SerializedCommandResponse;
import io.axoniq.axonserver.refactoring.messaging.command.api.CommandResponse;
import io.axoniq.axonserver.refactoring.transport.Mapper;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * @author Marc Gathier
 */
class GrpcCommandResponse implements CommandResponse {

    private final Mapper<SerializedObject, io.axoniq.axonserver.grpc.SerializedObject> serializedObjectMapper;
    private final SerializedCommandResponse serializedCommandResponse;

    public GrpcCommandResponse(
            Mapper<SerializedObject, io.axoniq.axonserver.grpc.SerializedObject> serializedObjectMapper,
            SerializedCommandResponse serializedCommandResponse) {
        this.serializedObjectMapper = serializedObjectMapper;
        this.serializedCommandResponse = serializedCommandResponse;
    }

    @Override
    public String requestId() {
        return serializedCommandResponse.getRequestIdentifier();
    }

    @Override
    public Message message() {
        return new Message() {
            @Override
            public String id() {
                return serializedCommandResponse.wrapped().getMessageIdentifier();
            }

            @Override
            public Optional<SerializedObject> payload() {
                if (serializedCommandResponse.wrapped().hasPayload()) {
                    return Optional.of(serializedObjectMapper
                                               .unmap(serializedCommandResponse.wrapped().getPayload()));
                }
                return Optional.empty();
            }

            @Override
            public <T> T metadata(String key) {
                return null;
            }

            @Override
            public Set<String> metadataKeys() {
                return Collections.emptySet();
            }
        };
    }

    @Override
    public Optional<Error> error() {
        if (!serializedCommandResponse.wrapped().getErrorCode().isEmpty()) {
            return Optional.of(new Error() {
                @Override
                public String code() {
                    return serializedCommandResponse.wrapped().getErrorCode();
                }

                @Override
                public String message() {
                    return serializedCommandResponse.wrapped().getErrorMessage().getMessage();
                }

                @Override
                public List<String> details() {
                    return serializedCommandResponse.wrapped().getErrorMessage().getDetailsList();
                }

                @Override
                public String source() {
                    return serializedCommandResponse.wrapped().getErrorMessage().getLocation();
                }
            });
        }
        return Optional.empty();
    }

    public SerializedCommandResponse serializedCommandResponse() {
        return serializedCommandResponse;
    }
}
