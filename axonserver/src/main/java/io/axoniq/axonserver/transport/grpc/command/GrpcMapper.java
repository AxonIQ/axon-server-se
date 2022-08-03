/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.transport.grpc.command;

import com.google.protobuf.ByteString;
import io.axoniq.axonserver.commandprocessing.spi.CommandResult;
import io.axoniq.axonserver.commandprocessing.spi.Metadata;
import io.axoniq.axonserver.commandprocessing.spi.Payload;
import io.axoniq.axonserver.commandprocessing.spi.ResultPayload;
import io.axoniq.axonserver.grpc.ErrorMessage;
import io.axoniq.axonserver.grpc.MetaDataValue;
import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.grpc.command.Command;
import io.axoniq.axonserver.grpc.command.CommandResponse;

import java.io.ByteArrayOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class GrpcMapper {

    public static Command map(io.axoniq.axonserver.commandprocessing.spi.Command command) {
        if (command instanceof GrpcCommand) {
            return ((GrpcCommand) command).wrapped();
        }

        Command.Builder builder = Command.newBuilder()
                                         .setMessageIdentifier(command.id())
                                         .setName(command.commandName());
        if (command.payload() != null) {
            builder.setPayload(map(command.payload()));
        }
        if (command.metadata() != null) {
            builder.putAllMetaData(map(command.metadata()));
        }
        return builder.build();
    }

    private static Map<String, MetaDataValue> map(Metadata metadata) {
        Map<String, MetaDataValue> values = new HashMap<>();
        metadata.metadataKeys().filter(GrpcMapper::isNotInternal)
                .collectMap(key -> key, k -> mapMetaDataValue(metadata.metadataValue(k).orElse(null)))
                .block();
        return values;
    }

    private static MetaDataValue mapMetaDataValue(Serializable s) {
        if (s instanceof Double) {
            return MetaDataValue.newBuilder().setDoubleValue((Double) s).build();
        }
        if (s instanceof Float) {
            return MetaDataValue.newBuilder().setDoubleValue((Float) s).build();
        }
        if (s instanceof Number) {
            return MetaDataValue.newBuilder().setNumberValue(((Number) s).longValue()).build();
        }
        if (s instanceof String) {
            return MetaDataValue.newBuilder().setTextValue((String) s).build();
        }
        if (s instanceof Boolean) {
            return MetaDataValue.newBuilder().setBooleanValue((Boolean) s).build();
        }
        if (s instanceof Payload) {
            return MetaDataValue.newBuilder().setBytesValue(map((Payload) s)).build();
        }
        throw new IllegalArgumentException("Invalid type");
    }

    private static boolean isNotInternal(String s) {
        return !s.startsWith("__");
    }

    public static CommandResponse map(CommandResult result) {
        if (result instanceof GrpcCommandResult) {
            return ((GrpcCommandResult) result).wrapped();
        }
        // TODO: 21/07/2022 Implement creating of a command response from command result
        CommandResponse.Builder builder = CommandResponse.newBuilder()
                                                         .setMessageIdentifier(result.id())
                                                         .setRequestIdentifier(result.commandId());
        ResultPayload payload = result.payload();
        if (payload != null) {
            if (payload.error()) {
                builder.setErrorMessage(ErrorMessage.newBuilder().setMessage("Error while executing command"));
            }
            if (payload.type() != null) {
                builder.setPayload(map(payload));
            }
        }
        if (result.metadata() != null) {
            builder.putAllMetaData(map(result.metadata()));
        }


        return builder.build();
    }

    private static SerializedObject map(Payload payload) {
        SerializedObject.Builder serializedObjectBuilder = SerializedObject.newBuilder();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        payload.data().toIterable().forEach(outputStream::write);
        serializedObjectBuilder.setData(ByteString.copyFrom(outputStream.toByteArray()));

        if (payload.type() != null) {
            serializedObjectBuilder.setType(payload.type());
        }
        return serializedObjectBuilder.build();
    }
}
