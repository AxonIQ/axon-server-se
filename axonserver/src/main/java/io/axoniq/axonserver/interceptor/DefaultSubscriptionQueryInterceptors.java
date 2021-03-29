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
import io.axoniq.axonserver.plugin.ExecutionContext;
import io.axoniq.axonserver.plugin.RequestRejectedException;
import io.axoniq.axonserver.plugin.ServiceWithInfo;
import io.axoniq.axonserver.plugin.interceptor.SubscriptionQueryRequestInterceptor;
import io.axoniq.axonserver.plugin.interceptor.SubscriptionQueryResponseInterceptor;
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

    private final PluginContextFilter pluginContextFilter;
    private final InterceptorTimer interceptorTimer;

    public DefaultSubscriptionQueryInterceptors(
            PluginContextFilter pluginContextFilter,
            MeterFactory meterFactory) {
        this.pluginContextFilter = pluginContextFilter;
        this.interceptorTimer = new InterceptorTimer(meterFactory);
    }


    @Override
    public SubscriptionQueryRequest subscriptionQueryRequest(SubscriptionQueryRequest subscriptionQueryRequest,
                                                             ExecutionContext executionContext) {
        List<ServiceWithInfo<SubscriptionQueryRequestInterceptor>> interceptors = pluginContextFilter
                .getServicesWithInfoForContext(
                        SubscriptionQueryRequestInterceptor.class, executionContext.contextName());
        if (interceptors.isEmpty()) {
            return subscriptionQueryRequest;
        }

        return interceptorTimer.time(executionContext.contextName(),
                                     "SubscriptionQueryRequestInterceptor",
                                     () -> {
                                         SubscriptionQueryRequest query = subscriptionQueryRequest;
                                         for (ServiceWithInfo<SubscriptionQueryRequestInterceptor> queryRequestInterceptor : interceptors) {
                                             try {
                                                 query = queryRequestInterceptor.service().subscriptionQueryRequest(
                                                         query,
                                                         executionContext);
                                             } catch (RequestRejectedException requestRejectedException) {
                                                 throw new MessagingPlatformException(ErrorCode.SUBSCRIPTION_QUERY_REJECTED_BY_INTERCEPTOR,
                                                                                      executionContext.contextName()
                                                                                              + " : request rejected by interceptor "
                                                                                              +
                                                                                              queryRequestInterceptor
                                                                                                      .pluginKey(),
                                                                                      requestRejectedException);
                                             } catch (Exception exception) {
                                                 throw new MessagingPlatformException(ErrorCode.EXCEPTION_IN_INTERCEPTOR,
                                                                                      executionContext.contextName()
                                                                                              + " : Exception thrown by the SubscriptionQueryRequestInterceptor in "
                                                                                              +
                                                                                              queryRequestInterceptor
                                                                                                      .pluginKey(),
                                                                                      exception);
                                             }
                                         }
                                         return query;
                                     });
    }

    @Override
    public SubscriptionQueryResponse subscriptionQueryResponse(SubscriptionQueryResponse subscriptionQueryResponse,
                                                               ExecutionContext executionContext) {
        List<ServiceWithInfo<SubscriptionQueryResponseInterceptor>> interceptors = pluginContextFilter
                .getServicesWithInfoForContext(
                        SubscriptionQueryResponseInterceptor.class,
                        executionContext.contextName()
                );
        if (interceptors.isEmpty()) {
            return subscriptionQueryResponse;
        }

        SubscriptionQueryResponse query = subscriptionQueryResponse;
        for (ServiceWithInfo<SubscriptionQueryResponseInterceptor> queryRequestInterceptor : interceptors) {
            try {
                query = queryRequestInterceptor.service().subscriptionQueryResponse(query, executionContext);
            } catch (Exception requestRejectedException) {
                throw new MessagingPlatformException(ErrorCode.EXCEPTION_IN_INTERCEPTOR,
                                                     executionContext.contextName()
                                                             + " : Exception thrown by the SubscriptionQueryResponseInterceptor in "
                                                             + queryRequestInterceptor.pluginKey(),
                                                     requestRejectedException);
            }
        }
        return query;
    }
}
