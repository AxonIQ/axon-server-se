/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.query;

import io.axoniq.axonserver.grpc.SerializedQuery;
import io.axoniq.axonserver.grpc.query.SubscriptionQueryRequest;
import io.axoniq.axonserver.message.ClientIdentification;

/**
 * @author Marc Gathier
 */
public class FakeQueryHandler extends QueryHandler {

    public FakeQueryHandler(ClientIdentification client, String componentName) {
        super(client, componentName);
    }

    @Override
    public void dispatch(SubscriptionQueryRequest query) {

    }

    @Override
    public void dispatch(SerializedQuery request, long timeout) {

    }
}
