/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.command;

import io.axoniq.axonserver.grpc.SerializedCommand;
import io.axoniq.axonserver.message.ClientIdentification;

/**
 * @author Marc Gathier
 */
public class WrappedCommand {
    private final ClientIdentification client;
    private final SerializedCommand command;
    private final long priority;

    public WrappedCommand(ClientIdentification client, SerializedCommand command) {
        this.client = client;
        this.command = command;
        this.priority = command.getPriority();
    }

    public SerializedCommand command() {
        return command;
    }

    public ClientIdentification client() {
        return client;
    }

    public long priority() {
        return priority;
    }

}
