/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.interceptor;

import io.axoniq.axonserver.grpc.SerializedQuery;
import io.axoniq.axonserver.grpc.query.QueryResponse;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.stereotype.Component;

/**
 * @author Marc Gathier
 */
@Component
@ConditionalOnMissingBean(QueryInterceptors.class)
public class NoOpQueryInterceptors implements QueryInterceptors {

    @Override
    public SerializedQuery queryRequest(InterceptorContext interceptorContext, SerializedQuery serializedQuery) {
        return serializedQuery;
    }

    @Override
    public QueryResponse queryResponse(InterceptorContext interceptorContext, QueryResponse response) {
        return response;
    }
}
