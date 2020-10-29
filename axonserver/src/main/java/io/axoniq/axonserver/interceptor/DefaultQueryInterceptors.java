/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.interceptor;

import io.axoniq.axonserver.config.OsgiController;
import io.axoniq.axonserver.extensions.interceptor.InterceptorContext;
import io.axoniq.axonserver.extensions.interceptor.OrderedInterceptor;
import io.axoniq.axonserver.extensions.interceptor.QueryRequestInterceptor;
import io.axoniq.axonserver.extensions.interceptor.QueryResponseInterceptor;
import io.axoniq.axonserver.grpc.SerializedQuery;
import io.axoniq.axonserver.grpc.query.QueryRequest;
import io.axoniq.axonserver.grpc.query.QueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author Marc Gathier
 */
@Component
public class DefaultQueryInterceptors implements QueryInterceptors {

    private final Logger logger = LoggerFactory.getLogger(DefaultQueryInterceptors.class);

    private final List<QueryRequestInterceptor> queryRequestInterceptors = new CopyOnWriteArrayList<>();
    private final List<QueryResponseInterceptor> queryResponseInterceptors = new CopyOnWriteArrayList<>();
    private final OsgiController osgiController;
    private volatile boolean initialized;

    public DefaultQueryInterceptors(OsgiController osgiController) {
        this.osgiController = osgiController;
    }

    private void initialize() {
        if (!initialized) {
            synchronized (osgiController) {
                if (initialized) {
                    return;
                }

                osgiController.getServices(QueryRequestInterceptor.class).forEach(queryRequestInterceptors::add);
                osgiController.getServices(QueryResponseInterceptor.class).forEach(queryResponseInterceptors::add);
                queryRequestInterceptors.sort(Comparator.comparingInt(OrderedInterceptor::order));
                queryResponseInterceptors.sort(Comparator.comparingInt(OrderedInterceptor::order));
                initialized = true;

                logger.info("{} queryRequestInterceptors", queryRequestInterceptors.size());
                logger.info("{} queryResponseInterceptors", queryResponseInterceptors.size());
            }
        }
    }

    @Override
    public SerializedQuery queryRequest(InterceptorContext interceptorContext, SerializedQuery serializedQuery) {
        initialize();
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
        initialize();
        for (QueryResponseInterceptor queryResponseInterceptor : queryResponseInterceptors) {
            response = queryResponseInterceptor.queryResponse(interceptorContext, response);
        }
        return response;
    }
}
