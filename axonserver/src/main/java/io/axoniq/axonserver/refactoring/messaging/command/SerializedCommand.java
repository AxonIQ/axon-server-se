/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.refactoring.messaging.command;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.grpc.command.Command;
import io.axoniq.axonserver.refactoring.messaging.ProcessingInstructionHelper;
import io.axoniq.axonserver.refactoring.transport.grpc.GrpcCommandMapping;

/**
 * Wrapper around gRPC {@link Command} to reduce serialization/deserialization.
 * @author Marc Gathier
 */
public class SerializedCommand {

    private final String clientStreamId;
    private final String messageId;
    private final byte[] serializedData;
    private final String context;
    private volatile Command command;

    public SerializedCommand(Command command) {
        this(command, null);
    }

    public SerializedCommand(Command command, String context) {
        this(command.toByteArray(), context);
        this.command = command;
    }

    public SerializedCommand(byte[] serializedCommand, String context) {
        this.serializedData = serializedCommand;
        this.clientStreamId = null;
        this.messageId = null;
        this.context = context;
    }

    public SerializedCommand(byte[] serializedCommand, String clientStreamId, String messageId) {
        this.serializedData = serializedCommand;
        this.clientStreamId = clientStreamId;
        this.messageId = messageId;
        this.context = null;
    }

    public static SerializedCommand getDefaultInstance() {
        return new SerializedCommand(Command.getDefaultInstance(), null);
    }


    public int getSerializedSize() {
        return serializedData.length;
    }

    public byte[] toByteArray() {
        return serializedData;
    }

    public Command wrapped() {
        if( command == null) {
            try {
                command = Command.parseFrom(serializedData);
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            }
        }
        return command;
    }


    public ByteString toByteString() {
        return ByteString.copyFrom(serializedData);
    }

    public String getMessageIdentifier() {
        if (messageId != null) {
            return messageId;
        }
        return wrapped().getMessageIdentifier();
    }

    public String getName() {
        return wrapped().getName();
    }

    public String getClientStreamId() {
        return clientStreamId;
    }

    public String getCommand() {
        return wrapped().getName();
    }

    public String getContext() {
        return context;
    }

    public String getRoutingKey() {
        return ProcessingInstructionHelper.routingKey(wrapped().getProcessingInstructionsList(),
                                                      wrapped().getMessageIdentifier());
    }

    public long getPriority() {
        return ProcessingInstructionHelper.priority(wrapped().getProcessingInstructionsList());
    }

    public io.axoniq.axonserver.refactoring.messaging.command.api.Command asCommand(String context) {
        return new GrpcCommandMapping(this);
    }
}
