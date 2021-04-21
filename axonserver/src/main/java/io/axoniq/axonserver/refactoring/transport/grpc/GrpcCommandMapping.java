/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.refactoring.transport.grpc;

import io.axoniq.axonserver.refactoring.messaging.api.Client;
import io.axoniq.axonserver.refactoring.messaging.api.Message;
import io.axoniq.axonserver.refactoring.messaging.api.SerializedObject;
import io.axoniq.axonserver.refactoring.messaging.command.SerializedCommand;
import io.axoniq.axonserver.refactoring.messaging.command.api.CommandDefinition;

import java.time.Instant;
import java.util.Optional;
import java.util.Set;

/**
 * @author Marc Gathier
 */
public class GrpcCommandMapping implements io.axoniq.axonserver.refactoring.messaging.command.api.Command {

    private final SerializedCommand serializedCommand;

    public GrpcCommandMapping(SerializedCommand serializedCommand) {
        this.serializedCommand = serializedCommand;
    }

    @Override
    public CommandDefinition definition() {
        return new CommandDefinition() {
            @Override
            public String name() {
                return serializedCommand.getName();
            }

            @Override
            public String context() {
                return serializedCommand.getContext();
            }
        };
    }

    @Override
    public Message message() {
        return new Message() {
            @Override
            public String id() {
                return serializedCommand.getMessageIdentifier();
            }

            @Override
            public Optional<SerializedObject> payload() {
                return Optional.of(new SerializedObjectMapping(serializedCommand.wrapped().getPayload()));
            }

            @Override
            public <T> T metadata(String key) {
                // TODO: 4/16/2021 convert metadatavalue to real object in a generic way
                return (T) serializedCommand.wrapped().getMetaDataMap().get(key);
            }

            @Override
            public Set<String> metadataKeys() {
                return serializedCommand.wrapped().getMetaDataMap().keySet();
            }
        };
    }

    @Override
    public String routingKey() {
        return serializedCommand.getRoutingKey();
    }

    @Override
    public Instant timestamp() {
        return Instant.ofEpochMilli(serializedCommand.wrapped().getTimestamp());
    }

    @Override
    public Client requester() {
        return new Client() {

            @Override
            public String id() {
                return serializedCommand.getClientStreamId();
            }

            @Override
            public String applicationName() {
                return serializedCommand.wrapped().getComponentName();
            }
        };
    }

    public SerializedCommand serializedCommand() {
        return serializedCommand;
    }
}
