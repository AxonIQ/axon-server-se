/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.interceptor;

import io.axoniq.axonserver.extensions.ExtensionUnitOfWork;
import io.axoniq.axonserver.extensions.RequestRejectedException;
import io.axoniq.axonserver.extensions.interceptor.SubscriptionQueryRequestInterceptor;
import io.axoniq.axonserver.extensions.interceptor.SubscriptionQueryResponseInterceptor;
import io.axoniq.axonserver.grpc.query.SubscriptionQueryRequest;
import io.axoniq.axonserver.grpc.query.SubscriptionQueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * @author Marc Gathier
 */
@Component
public class DefaultSubscriptionQueryInterceptors implements SubscriptionQueryInterceptors {

    private final Logger logger = LoggerFactory.getLogger(DefaultSubscriptionQueryInterceptors.class);
    private final ExtensionContextFilter extensionContextFilter;

    public DefaultSubscriptionQueryInterceptors(
            ExtensionContextFilter extensionContextFilter) {
        this.extensionContextFilter = extensionContextFilter;
    }


    @Override
    public SubscriptionQueryRequest subscriptionQueryRequest(SubscriptionQueryRequest subscriptionQueryRequest,
                                                             ExtensionUnitOfWork extensionContext)
            throws RequestRejectedException {

        SubscriptionQueryRequest query = subscriptionQueryRequest;
        for (SubscriptionQueryRequestInterceptor queryRequestInterceptor : extensionContextFilter.getServicesForContext(
                SubscriptionQueryRequestInterceptor.class, extensionContext.context())) {
            query = queryRequestInterceptor.subscriptionQueryRequest(query, extensionContext);
        }
        return query;
    }

    @Override
    public SubscriptionQueryResponse subscriptionQueryResponse(SubscriptionQueryResponse subscriptionQueryResponse,
                                                               ExtensionUnitOfWork extensionContext) {
        SubscriptionQueryResponse query = subscriptionQueryResponse;
        for (SubscriptionQueryResponseInterceptor queryRequestInterceptor : extensionContextFilter
                .getServicesForContext(
                        SubscriptionQueryResponseInterceptor.class,
                        extensionContext.context()
                )) {
            query = queryRequestInterceptor.subscriptionQueryResponse(query, extensionContext);
        }
        return query;
    }
}
