/*
 *  Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.command;

import io.axoniq.axonserver.grpc.SerializedCommand;
import io.axoniq.axonserver.message.ClientStreamIdentification;
import io.axoniq.axonserver.message.FlowControlQueues;

import java.util.function.Consumer;

/**
 * @author Marc Gathier
 */
public class DirectCommandHandler extends CommandHandler {

    private final FlowControlQueues<WrappedCommand> flowControlQueues;
    private final Consumer<String> cancelCallback;

    public DirectCommandHandler(
            ClientStreamIdentification clientStreamIdentification,
            FlowControlQueues<WrappedCommand> flowControlQueues,
            String clientId,
            String componentName) {
        this(clientStreamIdentification, flowControlQueues, clientId, componentName, r -> {
        });
    }

    public DirectCommandHandler(
            ClientStreamIdentification clientStreamIdentification,
            FlowControlQueues<WrappedCommand> flowControlQueues,
            String clientId,
            String componentName,
            Consumer<String> cancelCallback) {
        super(clientStreamIdentification, clientId, componentName);
        this.flowControlQueues = flowControlQueues;
        this.cancelCallback = cancelCallback;
    }


    @Override
    public void dispatch(SerializedCommand command) {
        WrappedCommand wrappedCommand = new WrappedCommand(clientStreamIdentification,
                                                           getClientId(),
                                                           command);

        flowControlQueues.put(queueName(), wrappedCommand, wrappedCommand.priority());
    }

    public String queueName() {
        return clientStreamIdentification.getClientStreamId();
    }

    @Override
    public void cancel(String requestIdentifier) {
        cancelCallback.accept(requestIdentifier);
    }
}
