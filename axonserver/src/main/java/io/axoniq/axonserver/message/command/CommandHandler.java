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

import java.util.Objects;

/**
 * @author Marc Gathier
 */
public abstract class CommandHandler implements Comparable<CommandHandler> {

    protected final ClientStreamIdentification clientStreamIdentification;
    private final String clientId;
    protected final String componentName;

    public CommandHandler(ClientStreamIdentification clientStreamIdentification,
                          String clientId, String componentName) {
        this.clientStreamIdentification = clientStreamIdentification;
        this.clientId = clientId;
        this.componentName = componentName;
    }

    public ClientStreamIdentification getClientStreamIdentification() {
        return clientStreamIdentification;
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
        return Objects.equals(clientStreamIdentification, that.clientStreamIdentification);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clientStreamIdentification);
    }

    @Override
    public int compareTo(CommandHandler o) {
        return clientStreamIdentification.compareTo(o.clientStreamIdentification);
    }

    /**
     * Dispatches the specified command to the handler.
     *
     * @param request the command request to be dispatched.
     */
    public abstract void dispatch(SerializedCommand request);

    public String getClientId() {
        return clientId;
    }

    public abstract String getMessagingServerName();

    public boolean isDirect() {
        return true;
    }

    public void close() {
    }
}
