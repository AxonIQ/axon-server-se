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
    private final InstructionAck acknowledgement;

    public SerializedCommandProviderInbound(CommandProviderInbound defaultInstance) {
        wrapped = defaultInstance;
        serializedCommand = defaultInstance.hasCommand() ? new SerializedCommand(defaultInstance.getCommand()) : null;
        acknowledgement = defaultInstance.hasAck() ? defaultInstance.getAck() : null;
    }

    public SerializedCommandProviderInbound(SerializedCommand serializedCommand, InstructionAck acknowledgement) {
        this.serializedCommand = serializedCommand;
        this.acknowledgement = acknowledgement;
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
        if (acknowledgement != null) {
            output.writeMessage(1, acknowledgement);
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
                .setAcknowledgement(acknowledgement);
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
            } else if( acknowledgement != null) {
                builder.setAck(acknowledgement);
            }
            wrapped = builder.build();
        }
        return wrapped;
    }

    public InstructionAckOrBuilder getInstructionResult() {
        return acknowledgement;
    }

    public SerializedCommand getSerializedCommand() {
        return serializedCommand;
    }

    public static class Builder extends GeneratedMessageV3.Builder<Builder> {
        private SerializedCommand serializedCommand;
        private InstructionAck acknowledgement;

        @Override
        protected GeneratedMessageV3.FieldAccessorTable internalGetFieldAccessorTable() {
            return null;
        }

        @Override
        public SerializedCommandProviderInbound build() {
            return new SerializedCommandProviderInbound(serializedCommand, acknowledgement);
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

        public Builder setAcknowledgement(InstructionAck acknowledgement) {
            this.acknowledgement = acknowledgement;
            return this;
        }
    }
}
