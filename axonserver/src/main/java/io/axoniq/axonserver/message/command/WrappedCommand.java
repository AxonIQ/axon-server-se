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
import io.axoniq.axonserver.message.ClientStreamIdentification;

/**
 * @author Marc Gathier
 */
public class WrappedCommand {

    private final ClientStreamIdentification clientStreamIdentification;
    private final String clientId;
    private final SerializedCommand command;
    private final long priority;

    public WrappedCommand(ClientStreamIdentification clientStreamIdentification, String clientId,
                          SerializedCommand command) {
        this.clientStreamIdentification = clientStreamIdentification;
        this.clientId = clientId;
        this.command = command;
        this.priority = command.getPriority();
    }

    public SerializedCommand command() {
        return command;
    }

    public ClientStreamIdentification client() {
        return clientStreamIdentification;
    }

    public long priority() {
        return priority;
    }

    public String targetClientId() {
        return clientId;
    }

    public String targetClientStreamId() {
        return clientStreamIdentification.getClientStreamId();
    }

    public String targetContext() {
        return clientStreamIdentification.getContext();
    }
}
