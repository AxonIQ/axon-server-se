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
public class ClientStreamIdentification implements Comparable<ClientStreamIdentification> {

    private static final Comparator<ClientStreamIdentification> COMPARATOR = Comparator
            .comparing(ClientStreamIdentification::getContext)
            .thenComparing(ClientStreamIdentification::getClientStreamId);
    private final String context;
    private final String clientStreamId;


    public ClientStreamIdentification(String context, String clientStreamId) {
        this.context = context;
        this.clientStreamId = clientStreamId;
    }

    public String getContext() {
        return context;
    }

    public String getClientStreamId() {
        return clientStreamId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ClientStreamIdentification that = (ClientStreamIdentification) o;
        return Objects.equals(context, that.context) &&
                Objects.equals(clientStreamId, that.clientStreamId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(context, clientStreamId);
    }

    public int compareTo(@Nonnull ClientStreamIdentification client) {
        return COMPARATOR.compare(this, client);
    }

    @Override
    public String toString() {
        return clientStreamId + "." + context;
    }

    public String metricName() {
        return clientStreamId + "." + context;
    }
}
