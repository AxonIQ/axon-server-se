/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.command;

import io.axoniq.axonserver.commandprocessing.spi.Command;
import io.axoniq.axonserver.commandprocessing.spi.CommandHandler;
import io.axoniq.axonserver.commandprocessing.spi.Metadata;
import io.axoniq.axonserver.commandprocessing.spi.Payload;
import org.jetbrains.annotations.NotNull;
import reactor.core.publisher.Flux;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public class CommandUtils {

    public static final String CONTEXT = "context";

    @NotNull
    public static Command getCommandRequest(String commandName) {
        return getCommandRequest(commandName, emptyMetadata());
    }

    public static Command getCommandRequest(String commandName, Metadata metadata) {
        return new Command() {
            private final Payload payload = emptyPayload();

            @Override
            public String id() {
                return "id";
            }

            @Override
            public String commandName() {
                return commandName;
            }

            @Override
            public String context() {
                return CONTEXT;
            }

            @Override
            public Payload payload() {
                return payload;
            }

            @Override
            public Metadata metadata() {
                return metadata;
            }
        };
    }

    @NotNull
    public static CommandHandler getCommandHandler(String commandName) {
        return new CommandHandler() {
            final String id = UUID.randomUUID().toString();
            final Metadata metadata = emptyMetadata();

            @Override
            public String id() {
                return id;
            }

            @Override
            public String description() {
                return null;
            }

            @Override
            public String commandName() {
                return commandName;
            }

            @Override
            public String context() {
                return CONTEXT;
            }

            @Override
            public Metadata metadata() {
                return metadata;
            }
        };
    }

    @NotNull
    public static Payload emptyPayload() {
        return new Payload() {

            @Override
            public String type() {
                return "Empty";
            }

            @Override
            public String contentType() {
                return null;
            }

            @Override
            public Flux<Byte> data() {
                return Flux.empty();
            }
        };
    }

    @NotNull
    public static Metadata emptyMetadata() {
        return metadata(Collections.emptyMap());
    }

    @NotNull
    public static Metadata metadata(Map<String, Serializable> data) {
        return new Metadata() {
            @Override
            public Iterable<String> metadataKeys() {
                return data.keySet();
            }

            @Override
            public <R extends Serializable> Optional<R> metadataValue(String metadataKey) {
                return (Optional<R>) Optional.ofNullable(data.get(metadataKey));
            }
        };
    }
}
