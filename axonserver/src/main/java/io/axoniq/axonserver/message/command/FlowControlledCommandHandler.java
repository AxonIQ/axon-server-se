/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.command;

import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.grpc.SerializedCommand;
import io.axoniq.axonserver.message.ClientStreamIdentification;
import io.axoniq.axonserver.message.FlowControlQueues;

/**
 * @author Marc Gathier
 */
public class FlowControlledCommandHandler extends CommandHandler {

    private final FlowControlQueues<WrappedCommand> commandQueues;
    private final CommandCache commandCache;

    public FlowControlledCommandHandler(FlowControlQueues<WrappedCommand> commandQueues,
                                        ClientStreamIdentification clientStreamIdentification, String clientId,
                                        String componentName, CommandCache commandCache) {
        super(clientStreamIdentification, clientId, componentName);
        this.commandQueues = commandQueues;
        this.commandCache = commandCache;
    }

    @Override
    public void dispatch(SerializedCommand request) {
        WrappedCommand wrappedCommand = new WrappedCommand(clientStreamIdentification,
                                                           getClientId(), request);
        commandQueues.put(clientStreamIdentification.toString(), wrappedCommand, wrappedCommand.priority() < 0);
    }

    @Override
    public String getMessagingServerName() {
        return null;
    }

    @Override
    public void close() {
        commandQueues.drain(clientStreamIdentification.toString(),
                            command -> {
                                if (commandCache != null) {
                                    commandCache.error(ErrorCode.CONNECTION_TO_HANDLER_LOST, command);
                                }
                            });
    }
}
