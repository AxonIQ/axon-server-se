/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.interceptor;

import io.axoniq.axonserver.extensions.OsgiController;
import io.axoniq.axonserver.extensions.ExtensionUnitOfWork;
import io.axoniq.axonserver.extensions.Ordered;
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
        osgiController.registerServiceListener(serviceEvent -> {
            logger.debug("service event {}", serviceEvent.getLocation());
            initialized = false;
        });
    }

    private void ensureInitialized() {
        if (!initialized) {
            synchronized (osgiController) {
                if (initialized) {
                    return;
                }
                queryRequestInterceptors.clear();
                queryResponseInterceptors.clear();

                osgiController.getServices(QueryRequestInterceptor.class).forEach(queryRequestInterceptors::add);
                osgiController.getServices(QueryResponseInterceptor.class).forEach(queryResponseInterceptors::add);
                queryRequestInterceptors.sort(Comparator.comparingInt(Ordered::order));
                queryResponseInterceptors.sort(Comparator.comparingInt(Ordered::order));
                initialized = true;

                logger.debug("{} queryRequestInterceptors", queryRequestInterceptors.size());
                logger.debug("{} queryResponseInterceptors", queryResponseInterceptors.size());
            }
        }
    }

    @Override
    public SerializedQuery queryRequest(SerializedQuery serializedQuery, ExtensionUnitOfWork interceptorContext) {
        ensureInitialized();
        if (queryRequestInterceptors.isEmpty()) {
            return serializedQuery;
        }
        QueryRequest query = serializedQuery.query();
        for (QueryRequestInterceptor queryRequestInterceptor : queryRequestInterceptors) {
            query = queryRequestInterceptor.queryRequest(query, interceptorContext);
        }
        return new SerializedQuery(serializedQuery.context(), serializedQuery.clientStreamId(), query);
    }

    @Override
    public QueryResponse queryResponse(QueryResponse response, ExtensionUnitOfWork interceptorContext) {
        ensureInitialized();
        for (QueryResponseInterceptor queryResponseInterceptor : queryResponseInterceptors) {
            response = queryResponseInterceptor.queryResponse(response, interceptorContext);
        }
        return response;
    }
}
