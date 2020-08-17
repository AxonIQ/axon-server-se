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
import io.grpc.stub.StreamObserver;

import java.util.Objects;

/**
 * @author Marc Gathier
 */
public abstract class CommandHandler<T> implements Comparable<CommandHandler<T>> {

    protected final StreamObserver<T> observer;
    protected final ClientStreamIdentification clientStreamIdentification;
    private final String clientId;
    protected final String componentName;

    public CommandHandler(StreamObserver<T> responseObserver, ClientStreamIdentification clientStreamIdentification,
                          String clientId, String componentName) {
        this.observer = responseObserver;
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
        CommandHandler<?> that = (CommandHandler<?>) o;
        return Objects.equals(observer, that.observer) &&
                Objects.equals(clientStreamIdentification, that.clientStreamIdentification);
    }

    @Override
    public int hashCode() {
        return Objects.hash(observer, clientStreamIdentification);
    }

    @Override
    public int compareTo(CommandHandler o) {
        int clientResult = clientStreamIdentification.compareTo(o.clientStreamIdentification);
        if (clientResult == 0) {
            clientResult = observer.toString().compareTo(o.observer.toString());
        }
        return clientResult;
    }

    /**
     * Dispatches the specified command to the handler.
     *
     * @param request the command request to be dispatched.
     */
    public abstract void dispatch(SerializedCommand request);

    public abstract void confirm(String messageId);

    public String queueName() {
        return clientStreamIdentification.toString();
    }

    public String getMessagingServerName() {
        return null;
    }

    public String getClientId() {
        return clientId;
    }
}
