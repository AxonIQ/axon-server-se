/*
 *  Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */
package io.axoniq.axonserver.grpc;

import java.util.Objects;

/**
 * Value object containing a client and a context.
 *
 * @author Marc Gathier
 * @since 4.5.3
 */
public class ClientContext {
    private final String clientId;
    private final String context;

    public ClientContext(String clientId, String context) {
        this.clientId = clientId;
        this.context = context;
    }

    public String clientId() {
        return clientId;
    }

    public String context() {
        return context;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ClientContext that = (ClientContext) o;
        return Objects.equals(clientId, that.clientId) && Objects.equals(context, that.context);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clientId, context);
    }

    @Override
    public String toString() {
        return clientId + "." + context;
    }
}
