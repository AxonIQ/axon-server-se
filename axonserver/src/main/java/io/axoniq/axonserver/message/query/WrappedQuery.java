/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.query;

import io.axoniq.axonserver.ProcessingInstructionHelper;
import io.axoniq.axonserver.grpc.SerializedQuery;
import io.axoniq.axonserver.message.ClientIdentification;

/**
 * @author Marc Gathier
 */
public class WrappedQuery {
    private final SerializedQuery queryRequest;
    private final ClientIdentification client;
    private final long timeout;
    private final long priority;

    public WrappedQuery(ClientIdentification client, SerializedQuery queryRequest, long timeout) {
        this.queryRequest = queryRequest;
        this.timeout = timeout;
        this.client = client;
        this.priority = ProcessingInstructionHelper.priority(queryRequest.query().getProcessingInstructionsList());
    }

    public SerializedQuery queryRequest() {
        return queryRequest;
    }

    public long timeout() {
        return timeout;
    }

    public long priority() {
        return priority;
    }

    public ClientIdentification client() {
        return client;
    }
}
