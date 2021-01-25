/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
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
import io.axoniq.axonserver.extensions.interceptor.SubscriptionQueryRequestInterceptor;
import io.axoniq.axonserver.extensions.interceptor.SubscriptionQueryResponseInterceptor;
import io.axoniq.axonserver.grpc.query.SubscriptionQueryRequest;
import io.axoniq.axonserver.grpc.query.SubscriptionQueryResponse;
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
public class DefaultSubscriptionQueryInterceptors implements SubscriptionQueryInterceptors {

    private final Logger logger = LoggerFactory.getLogger(DefaultSubscriptionQueryInterceptors.class);

    private final List<ServiceWithInfo<SubscriptionQueryRequestInterceptor>> queryRequestInterceptors = new CopyOnWriteArrayList<>();
    private final List<ServiceWithInfo<SubscriptionQueryResponseInterceptor>> queryResponseInterceptors = new CopyOnWriteArrayList<>();

    private final ExtensionServiceProvider extensionServiceProvider;
    private final ExtensionContextFilter extensionContextFilter;
    private volatile boolean initialized;

    public DefaultSubscriptionQueryInterceptors(ExtensionServiceProvider extensionServiceProvider,
                                                ExtensionContextFilter extensionContextFilter) {
        this.extensionServiceProvider = extensionServiceProvider;
        this.extensionContextFilter = extensionContextFilter;
        extensionServiceProvider.registerExtensionListener(serviceEvent -> {
            logger.debug("service event {}", serviceEvent);
            initialized = false;
        });
    }

    private void ensureInitialized() {
        if (!initialized) {
            synchronized (extensionServiceProvider) {
                if (initialized) {
                    return;
                }
                initHooks(SubscriptionQueryRequestInterceptor.class, queryRequestInterceptors);
                initHooks(SubscriptionQueryResponseInterceptor.class, queryResponseInterceptors);

                initialized = true;
            }
        }
    }

    private <T extends Ordered> void initHooks(Class<T> clazz, List<ServiceWithInfo<T>> hooks) {
        hooks.clear();
        hooks.addAll(extensionServiceProvider.getServicesWithInfo(clazz));
        hooks.sort(Comparator.comparingInt(ServiceWithInfo::order));
        logger.debug("{} {}}", hooks.size(), clazz.getSimpleName());
    }

    @Override
    public SubscriptionQueryRequest subscriptionQueryRequest(SubscriptionQueryRequest subscriptionQueryRequest,
                                                             ExtensionUnitOfWork extensionContext) {
        ensureInitialized();
        SubscriptionQueryRequest query = subscriptionQueryRequest;
        for (ServiceWithInfo<SubscriptionQueryRequestInterceptor> queryRequestInterceptor : queryRequestInterceptors) {
            if (extensionContextFilter.test(extensionContext.context(), queryRequestInterceptor.extensionKey())) {
                query = queryRequestInterceptor.service().subscriptionQueryRequest(query, extensionContext);
            }
        }
        return query;
    }

    @Override
    public SubscriptionQueryResponse subscriptionQueryResponse(SubscriptionQueryResponse subscriptionQueryResponse,
                                                               ExtensionUnitOfWork extensionContext) {
        ensureInitialized();
        SubscriptionQueryResponse query = subscriptionQueryResponse;
        try {
            for (ServiceWithInfo<SubscriptionQueryResponseInterceptor> queryRequestInterceptor : queryResponseInterceptors) {
                if (extensionContextFilter.test(extensionContext.context(), queryRequestInterceptor.extensionKey())) {
                    query = queryRequestInterceptor.service().subscriptionQueryResponse(query, extensionContext);
                }
            }
        } catch (Exception ex) {
            logger.warn("{}: an exception occurred in a SubscriptionQueryResponseInterceptor",
                        extensionContext.context(),
                        ex);
        }
        return query;
    }
}
