/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message;

import java.util.Comparator;
import java.util.Objects;
import javax.annotation.Nonnull;

/**
 * @author Marc Gathier
 */
public class ClientIdentification implements Comparable<ClientIdentification> {

    private static final Comparator<ClientIdentification> COMPARATOR = Comparator
            .comparing(ClientIdentification::getContext).thenComparing(ClientIdentification::getClientId);
    private final String context;
    private final String clientId;


    public ClientIdentification(String context, String clientId) {
        this.context = context;
        this.clientId = clientId;
    }

    public String getContext() {
        return context;
    }

    public String getClientId() {
        return clientId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ClientIdentification that = (ClientIdentification) o;
        return Objects.equals(context, that.context) &&
                Objects.equals(clientId, that.clientId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(context, clientId);
    }

    public int compareTo(@Nonnull ClientIdentification client) {
        return COMPARATOR.compare(this, client);
    }

    @Override
    public String toString() {
        return clientId + "." + context;
    }

    public String metricName() {
        return clientId + "." + context;
    }
}
