/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.grpc;

import com.google.protobuf.AbstractParser;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.ExtensionRegistryLite;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import com.google.protobuf.WireFormat;
import io.axoniq.axonserver.grpc.command.CommandProviderInbound;

import java.io.IOException;

/**
 * Wrapper around CommandProviderInbound that maintains serialized data to reduce time to write to stream.
 *
 * @author Marc Gathier
 */
public class SerializedCommandProviderInbound extends SerializedMessage<CommandProviderInbound> {
    private static final SerializedCommandProviderInbound DEFAULT_INSTANCE = new SerializedCommandProviderInbound(CommandProviderInbound.getDefaultInstance());
    private volatile CommandProviderInbound wrapped;
    private final SerializedCommand serializedCommand;
    private final Confirmation confirmation;

    public SerializedCommandProviderInbound(CommandProviderInbound defaultInstance) {
        wrapped = defaultInstance;
        serializedCommand = defaultInstance.hasCommand() ? new SerializedCommand(defaultInstance.getCommand()) : null;
        confirmation = defaultInstance.hasConfirmation() ? defaultInstance.getConfirmation() : null;
    }

    public SerializedCommandProviderInbound(SerializedCommand serializedCommand, Confirmation confirmation) {
        this.serializedCommand = serializedCommand;
        this.confirmation = confirmation;
    }

    public static SerializedCommandProviderInbound getDefaultInstance() {
        return DEFAULT_INSTANCE;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    @Override
    public Parser<? extends Message> getParserForType() {
        return new AbstractParser<Message>() {
            @Override
            public Message parsePartialFrom(CodedInputStream codedInputStream,
                                            ExtensionRegistryLite extensionRegistryLite)
                    throws InvalidProtocolBufferException {
                // is never parsed in the server
                try {
                    return new SerializedCommandProviderInbound(CommandProviderInbound.parseFrom(codedInputStream));
                } catch (IOException e) {
                    throw new InvalidProtocolBufferException(e);
                }
            }
        };
    }

    @Override
    public void writeTo(CodedOutputStream output) throws IOException {
        if (confirmation != null) {
            output.writeMessage(1, confirmation);
        }
        if (serializedCommand != null) {
            output.writeTag(2, WireFormat.WIRETYPE_LENGTH_DELIMITED); // max 5 bytes
            output.writeUInt32NoTag(serializedCommand.getSerializedSize()); // max 5 bytes
            output.writeRawBytes(serializedCommand.toByteArray());
        }
        output.flush();
    }

    @Override
    public Message.Builder newBuilderForType() {
        return newBuilder();
    }

    @Override
    public Message.Builder toBuilder() {
        return new Builder()
                .setCommand(serializedCommand)
                .setConfirmation(confirmation);
    }

    @Override
    public Message getDefaultInstanceForType() {
        return DEFAULT_INSTANCE;
    }

    protected CommandProviderInbound wrapped() {
        if( wrapped == null) {
            CommandProviderInbound.Builder builder = CommandProviderInbound.newBuilder();
            if( serializedCommand != null) {
                builder.setCommand(serializedCommand.wrapped());
            } else if( confirmation != null) {
                builder.setConfirmation(confirmation);
            }
            wrapped = builder.build();
        }
        return wrapped;
    }

    public ConfirmationOrBuilder getConfirmation() {
        return confirmation;
    }

    public SerializedCommand getSerializedCommand() {
        return serializedCommand;
    }

    public static class Builder extends GeneratedMessageV3.Builder<Builder> {
        private SerializedCommand serializedCommand;
        private Confirmation confirmation;

        @Override
        protected GeneratedMessageV3.FieldAccessorTable internalGetFieldAccessorTable() {
            return null;
        }

        @Override
        public SerializedCommandProviderInbound build() {
            return new SerializedCommandProviderInbound(serializedCommand, confirmation);
        }

        @Override
        public SerializedCommandProviderInbound buildPartial() {
            return null;
        }

        @Override
        public Message getDefaultInstanceForType() {
            return null;
        }

        public Builder setCommand(SerializedCommand request) {
            serializedCommand = request;
            return this;
        }

        public Builder setConfirmation(Confirmation messageId) {
            confirmation = messageId;
            return this;
        }
    }
}
