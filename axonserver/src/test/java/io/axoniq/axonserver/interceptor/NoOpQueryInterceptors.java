/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.interceptor;

import io.axoniq.axonserver.grpc.SerializedQuery;
import io.axoniq.axonserver.grpc.query.QueryResponse;
import io.axoniq.axonserver.plugin.PluginUnitOfWork;

/**
 * @author Marc Gathier
 */
public class NoOpQueryInterceptors implements QueryInterceptors {

    @Override
    public SerializedQuery queryRequest(SerializedQuery serializedQuery, PluginUnitOfWork unitOfWork) {
        return serializedQuery;
    }

    @Override
    public QueryResponse queryResponse(QueryResponse response, PluginUnitOfWork unitOfWork) {
        return response;
    }
}
