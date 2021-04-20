/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.refactoring.transport.rest.dto;

import io.axoniq.axonserver.grpc.ProcessingInstruction;
import io.axoniq.axonserver.grpc.command.Command;
import io.axoniq.axonserver.refactoring.messaging.ProcessingInstructionHelper;
import io.axoniq.axonserver.refactoring.messaging.api.Message;
import io.axoniq.axonserver.refactoring.messaging.api.SerializedObject;
import io.axoniq.axonserver.refactoring.messaging.command.api.CommandDefinition;
import io.axoniq.axonserver.refactoring.util.StringUtils;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import javax.validation.constraints.NotNull;

/**
 * @author Marc Gathier
 */
public class CommandRequestJson {
    private String messageIdentifier;
    @NotNull(message = "'name' field cannot be missing")
    private String name;
    @NotNull(message = "'routingKey' field cannot be missing")
    private String routingKey;
    private long timestamp;
    @NotNull(message = "'payload' of a command cannot be missing")
    private SerializedObjectJson payload;
    private MetaDataJson metaData = new MetaDataJson();

    public Command asCommand() {
        Command.Builder builder = Command.newBuilder()
                                         .setName(name)
                                         .setMessageIdentifier(StringUtils.getOrDefault(messageIdentifier,
                                                                                        UUID.randomUUID().toString()))
                                         .setTimestamp(timestamp);
        if( payload != null) {
            builder.setPayload(payload.asSerializedObject());
        }

        return builder.putAllMetaData(metaData.asMetaDataValueMap())
                      .addAllProcessingInstructions(processingInstructions())
                      .build();
    }

    private Iterable<? extends ProcessingInstruction> processingInstructions() {
        List<ProcessingInstruction> processingInstructions = new ArrayList<>();
        processingInstructions.add(ProcessingInstructionHelper.routingKey(routingKey));
        return processingInstructions;
    }

    public String getMessageIdentifier() {
        return messageIdentifier;
    }

    public void setMessageIdentifier(String messageIdentifier) {
        this.messageIdentifier = messageIdentifier;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public SerializedObjectJson getPayload() {
        return payload;
    }

    public void setPayload(SerializedObjectJson payload) {
        this.payload = payload;
    }

    public MetaDataJson getMetaData() {
        return metaData;
    }

    public void setMetaData(MetaDataJson metaData) {
        this.metaData = metaData;
    }

    public String getRoutingKey() {
        return routingKey;
    }

    public void setRoutingKey(String routingKey) {
        this.routingKey = routingKey;
    }


    public io.axoniq.axonserver.refactoring.messaging.command.api.Command asCommandFor(String context) {
        return new io.axoniq.axonserver.refactoring.messaging.command.api.Command() {
            @Override
            public CommandDefinition definition() {
                return new CommandDefinition() {
                    @Override
                    public String name() {
                        return name;
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
                        return messageIdentifier;
                    }

                    @Override
                    public Optional<SerializedObject> payload() {
                        return Optional.of(new SerializedObject() {
                            @Override
                            public String type() {
                                return payload.getType();
                            }

                            @Override
                            public String revision() {
                                return payload.getRevision();
                            }

                            @Override
                            public byte[] data() {
                                return payload.getData().getBytes();
                            }
                        });
                    }

                    @Override
                    public <T> T metadata(String key) {
                        return (T) metaData.get(key);
                    }

                    @Override
                    public Set<String> metadataKeys() {
                        return metaData.keySet();
                    }
                };
            }

            @Override
            public String routingKey() {
                return routingKey;
            }

            @Override
            public Instant timestamp() {
                return Instant.ofEpochMilli(timestamp);
            }
        };
    }
}
