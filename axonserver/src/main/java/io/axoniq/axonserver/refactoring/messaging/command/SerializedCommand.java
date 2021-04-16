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
import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.grpc.command.Command;
import io.axoniq.axonserver.refactoring.messaging.ProcessingInstructionHelper;
import io.axoniq.axonserver.refactoring.messaging.api.Client;
import io.axoniq.axonserver.refactoring.messaging.api.Message;
import io.axoniq.axonserver.refactoring.messaging.api.Payload;
import io.axoniq.axonserver.refactoring.messaging.command.api.CommandDefinition;
import io.axoniq.axonserver.refactoring.transport.grpc.SerializedObjectMapping;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.util.Optional;
import java.util.Set;

/**
 * Wrapper around gRPC {@link Command} to reduce serialization/deserialization.
 * @author Marc Gathier
 */
public class SerializedCommand  {

    private final String clientStreamId;
    private final String messageId;
    private final byte[] serializedData;
    private volatile Command command;

    public SerializedCommand(Command command) {
        this(command.toByteArray());
        this.command = command;
    }

    public SerializedCommand(byte[] serializedCommand) {
        this.serializedData = serializedCommand;
        this.clientStreamId = null;
        this.messageId = null;
    }

    public SerializedCommand(byte[] serializedCommand, String clientStreamId, String messageId) {
        this.serializedData = serializedCommand;
        this.clientStreamId = clientStreamId;
        this.messageId = messageId;
    }

    public static SerializedCommand getDefaultInstance() {
        return new SerializedCommand(Command.getDefaultInstance());
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

    public String getRoutingKey() {
        return ProcessingInstructionHelper.routingKey(wrapped().getProcessingInstructionsList(), wrapped().getMessageIdentifier());
    }

    public long getPriority() {
        return ProcessingInstructionHelper.priority(wrapped().getProcessingInstructionsList());
    }

    public io.axoniq.axonserver.refactoring.messaging.command.api.Command asCommand(String context) {
        return new io.axoniq.axonserver.refactoring.messaging.command.api.Command() {
            @Override
            public CommandDefinition definition() {
                return new CommandDefinition() {
                    @Override
                    public String name() {
                        return getName();
                    }

                    @Override
                    public String context() {
                        return context;
                    }
                };
            }

            @Override
            public Message message() {
                return new Message() {
                    @Override
                    public String id() {
                        return getMessageIdentifier();
                    }

                    @Override
                    public Optional<Payload> payload() {
                        return Optional.of(new SerializedObjectMapping(wrapped().getPayload()));
                    }

                    @Override
                    public <T> T metadata(String key) {
                        // TODO: 4/16/2021 convert metadatavalue to real object in a generic way
                        return (T) wrapped().getMetaDataMap().get(key);
                    }

                    @Override
                    public Set<String> metadataKeys() {
                        return wrapped().getMetaDataMap().keySet();
                    }
                };
            }

            @Override
            public String routingKey() {
                return getRoutingKey();
            }

            @Override
            public Instant timestamp() {
                return Instant.ofEpochMilli(wrapped().getTimestamp());
            }

            @Override
            public Client requester() {
                return new Client() {

                    @Override
                    public String id() {
                        return getClientStreamId();
                    }

                    @Override
                    public String applicationName() {
                        return wrapped().getComponentName();
                    }
                };
            }
        };
    }
}
