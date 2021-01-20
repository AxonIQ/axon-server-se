/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.interceptor;

import io.axoniq.axonserver.extensions.ExtensionServiceProvider;
import io.axoniq.axonserver.extensions.ExtensionUnitOfWork;
import io.axoniq.axonserver.extensions.Ordered;
import io.axoniq.axonserver.extensions.ServiceWithInfo;
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
 * Bundles all interceptors for query handling in a single class.
 *
 * @author Marc Gathier
 * @since 4.5
 */
@Component
public class DefaultQueryInterceptors implements QueryInterceptors {

    private final Logger logger = LoggerFactory.getLogger(DefaultQueryInterceptors.class);

    private final List<ServiceWithInfo<QueryRequestInterceptor>> queryRequestInterceptors = new CopyOnWriteArrayList<>();
    private final List<ServiceWithInfo<QueryResponseInterceptor>> queryResponseInterceptors = new CopyOnWriteArrayList<>();

    private final ExtensionServiceProvider osgiController;
    private final ExtensionContextFilter extensionContextFilter;
    private volatile boolean initialized;

    public DefaultQueryInterceptors(ExtensionServiceProvider osgiController,
                                    ExtensionContextFilter extensionContextFilter) {
        this.osgiController = osgiController;
        this.extensionContextFilter = extensionContextFilter;
        osgiController.registerExtensionListener(serviceEvent -> {
            logger.debug("service event {}", serviceEvent);
            initialized = false;
        });
    }

    private void ensureInitialized() {
        if (!initialized) {
            synchronized (osgiController) {
                if (initialized) {
                    return;
                }
                initHooks(QueryRequestInterceptor.class, queryRequestInterceptors);
                initHooks(QueryResponseInterceptor.class, queryResponseInterceptors);

                initialized = true;
            }
        }
    }

    private <T extends Ordered> void initHooks(Class<T> clazz, List<ServiceWithInfo<T>> hooks) {
        hooks.clear();
        hooks.addAll(osgiController.getServicesWithInfo(clazz));
        hooks.sort(Comparator.comparingInt(ServiceWithInfo::order));
        logger.debug("{} {}}", hooks.size(), clazz.getSimpleName());
    }

    @Override
    public SerializedQuery queryRequest(SerializedQuery serializedQuery, ExtensionUnitOfWork extensionUnitOfWork) {
        ensureInitialized();
        if (queryRequestInterceptors.isEmpty()) {
            return serializedQuery;
        }
        QueryRequest query = serializedQuery.query();
        for (ServiceWithInfo<QueryRequestInterceptor> queryRequestInterceptor : queryRequestInterceptors) {
            if (extensionContextFilter.test(extensionUnitOfWork.context(), queryRequestInterceptor.extensionKey())) {
                query = queryRequestInterceptor.service().queryRequest(query, extensionUnitOfWork);
            }
        }
        return new SerializedQuery(serializedQuery.context(), serializedQuery.clientStreamId(), query);
    }

    @Override
    public QueryResponse queryResponse(QueryResponse response, ExtensionUnitOfWork extensionUnitOfWork) {
        ensureInitialized();
        try {
            for (ServiceWithInfo<QueryResponseInterceptor> queryResponseInterceptor : queryResponseInterceptors) {
                if (extensionContextFilter.test(extensionUnitOfWork.context(),
                                                queryResponseInterceptor.extensionKey())) {
                    response = queryResponseInterceptor.service().queryResponse(response, extensionUnitOfWork);
                }
            }
        } catch (Exception ex) {
            logger.warn("{}@{} an exception occurred in a QueryResponseInterceptor", extensionUnitOfWork.principal(),
                        extensionUnitOfWork.context(), ex);
        }
        return response;
    }
}
