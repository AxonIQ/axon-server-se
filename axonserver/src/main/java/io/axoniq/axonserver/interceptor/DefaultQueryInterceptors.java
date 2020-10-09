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
import io.axoniq.axonserver.grpc.query.QueryRequest;
import io.axoniq.axonserver.grpc.query.QueryResponse;
import io.axoniq.axonserver.util.ObjectUtils;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;

/**
 * @author Marc Gathier
 */
@Component
public class DefaultQueryInterceptors implements QueryInterceptors {

    private final List<QueryRequestInterceptor> queryRequestInterceptors;
    private final List<QueryResponseInterceptor> queryResponseInterceptors;

    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    public DefaultQueryInterceptors(
            @Nullable List<QueryRequestInterceptor> queryRequestInterceptors,
            @Nullable List<QueryResponseInterceptor> queryResponseInterceptors) {
        this.queryRequestInterceptors = ObjectUtils.getOrDefault(queryRequestInterceptors, Collections.emptyList());
        this.queryResponseInterceptors = ObjectUtils.getOrDefault(queryResponseInterceptors, Collections.emptyList());
    }

    @Override
    public SerializedQuery queryRequest(InterceptorContext interceptorContext, SerializedQuery serializedQuery) {
        if (queryRequestInterceptors.isEmpty()) {
            return serializedQuery;
        }
        QueryRequest query = serializedQuery.query();
        for (QueryRequestInterceptor queryRequestInterceptor : queryRequestInterceptors) {
            query = queryRequestInterceptor.queryRequest(interceptorContext, query);
        }
        return new SerializedQuery(serializedQuery.context(), serializedQuery.clientStreamId(), query);
    }

    @Override
    public QueryResponse queryResponse(InterceptorContext interceptorContext, QueryResponse response) {
        for (QueryResponseInterceptor queryResponseInterceptor : queryResponseInterceptors) {
            response = queryResponseInterceptor.queryResponse(interceptorContext, response);
        }
        return response;
    }
}
