/*
 * Copyright (c) 2017-2021 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.interceptor;

import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.extensions.ExtensionUnitOfWork;
import io.axoniq.axonserver.extensions.RequestRejectedException;
import io.axoniq.axonserver.extensions.ServiceWithInfo;
import io.axoniq.axonserver.extensions.interceptor.SubscriptionQueryRequestInterceptor;
import io.axoniq.axonserver.extensions.interceptor.SubscriptionQueryResponseInterceptor;
import io.axoniq.axonserver.grpc.query.SubscriptionQueryRequest;
import io.axoniq.axonserver.grpc.query.SubscriptionQueryResponse;
import io.axoniq.axonserver.metric.MeterFactory;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author Marc Gathier
 */
@Component
public class DefaultSubscriptionQueryInterceptors implements SubscriptionQueryInterceptors {

    private final ExtensionContextFilter extensionContextFilter;
    private final InterceptorTimer interceptorTimer;

    public DefaultSubscriptionQueryInterceptors(
            ExtensionContextFilter extensionContextFilter,
            MeterFactory meterFactory) {
        this.extensionContextFilter = extensionContextFilter;
        this.interceptorTimer = new InterceptorTimer(meterFactory);
    }


    @Override
    public SubscriptionQueryRequest subscriptionQueryRequest(SubscriptionQueryRequest subscriptionQueryRequest,
                                                             ExtensionUnitOfWork extensionContext) {
        List<ServiceWithInfo<SubscriptionQueryRequestInterceptor>> interceptors = extensionContextFilter
                .getServicesWithInfoForContext(
                        SubscriptionQueryRequestInterceptor.class, extensionContext.context());
        if (interceptors.isEmpty()) {
            return subscriptionQueryRequest;
        }

        return interceptorTimer.time(extensionContext.context(),
                                     "SubscriptionQueryRequestInterceptor",
                                     () -> {
                                         SubscriptionQueryRequest query = subscriptionQueryRequest;
                                         for (ServiceWithInfo<SubscriptionQueryRequestInterceptor> queryRequestInterceptor : interceptors) {
                                             try {
                                                 query = queryRequestInterceptor.service().subscriptionQueryRequest(
                                                         query,
                                                         extensionContext);
                                             } catch (RequestRejectedException requestRejectedException) {
                                                 throw new MessagingPlatformException(ErrorCode.SUBSCRIPTION_QUERY_REJECTED_BY_INTERCEPTOR,
                                                                                      extensionContext.context()
                                                                                              + " : request rejected by interceptor "
                                                                                              +
                                                                                              queryRequestInterceptor
                                                                                                      .extensionKey(),
                                                                                      requestRejectedException);
                                             } catch (Exception requestRejectedException) {
                                                 throw new MessagingPlatformException(ErrorCode.EXCEPTION_IN_INTERCEPTOR,
                                                                                      extensionContext.context()
                                                                                              + " : Exception thrown by the SubscriptionQueryRequestInterceptor in "
                                                                                              +
                                                                                              queryRequestInterceptor
                                                                                                      .extensionKey(),
                                                                                      requestRejectedException);
                                             }
                                         }
                                         return query;
                                     });
    }

    @Override
    public SubscriptionQueryResponse subscriptionQueryResponse(SubscriptionQueryResponse subscriptionQueryResponse,
                                                               ExtensionUnitOfWork extensionContext) {
        List<ServiceWithInfo<SubscriptionQueryResponseInterceptor>> interceptors = extensionContextFilter
                .getServicesWithInfoForContext(
                        SubscriptionQueryResponseInterceptor.class,
                        extensionContext.context()
                );
        if (interceptors.isEmpty()) {
            return subscriptionQueryResponse;
        }

        SubscriptionQueryResponse query = subscriptionQueryResponse;
        for (ServiceWithInfo<SubscriptionQueryResponseInterceptor> queryRequestInterceptor : interceptors) {
            try {
                query = queryRequestInterceptor.service().subscriptionQueryResponse(query, extensionContext);
            } catch (Exception requestRejectedException) {
                throw new MessagingPlatformException(ErrorCode.EXCEPTION_IN_INTERCEPTOR,
                                                     extensionContext.context()
                                                             + " : Exception thrown by the SubscriptionQueryResponseInterceptor in "
                                                             +
                                                             queryRequestInterceptor.extensionKey(),
                                                     requestRejectedException);
            }
        }
        return query;
    }
}
