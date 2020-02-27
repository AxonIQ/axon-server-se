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

import java.util.Objects;

/**
 * @author Marc Gathier
 */
public abstract class CommandHandler implements Comparable<CommandHandler> {

    protected final ClientIdentification client;
    protected final String componentName;

    public CommandHandler(ClientIdentification client, String componentName) {
        this.client = client;
        this.componentName = componentName;
    }

    public ClientIdentification getClient() {
        return client;
    }

    public String getComponentName() {
        return componentName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CommandHandler that = (CommandHandler) o;
        return Objects.equals(client, that.client);
    }

    @Override
    public int hashCode() {
        return Objects.hash(client);
    }

    @Override
    public int compareTo(CommandHandler o) {
        return client.compareTo(o.client);
    }

    public abstract void dispatch( SerializedCommand request);

    public String axonServerName() {
        return null;
    }
}
