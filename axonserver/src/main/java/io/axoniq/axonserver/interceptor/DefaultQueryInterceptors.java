/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
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
import io.axoniq.axonserver.plugin.interceptor.QueryRequestInterceptor;
import io.axoniq.axonserver.plugin.interceptor.QueryResponseInterceptor;
import io.axoniq.axonserver.grpc.SerializedQuery;
import io.axoniq.axonserver.grpc.query.QueryRequest;
import io.axoniq.axonserver.grpc.query.QueryResponse;
import io.axoniq.axonserver.metric.MeterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Bundles all interceptors for query handling in a single class.
 *
 * @author Marc Gathier
 * @since 4.5
 */
@Component
public class DefaultQueryInterceptors implements QueryInterceptors {

    private final Logger logger = LoggerFactory.getLogger(DefaultQueryInterceptors.class);

    private final PluginContextFilter pluginContextFilter;
    private final InterceptorTimer interceptorTimer;

    public DefaultQueryInterceptors(
            PluginContextFilter pluginContextFilter,
            MeterFactory meterFactory) {
        this.pluginContextFilter = pluginContextFilter;
        this.interceptorTimer = new InterceptorTimer(meterFactory);
    }


    @Override
    public SerializedQuery queryRequest(SerializedQuery serializedQuery, ExecutionContext executionContext) {
        List<ServiceWithInfo<QueryRequestInterceptor>> queryRequestInterceptors = pluginContextFilter
                .getServicesWithInfoForContext(
                        QueryRequestInterceptor.class,
                        executionContext.contextName());
        if (queryRequestInterceptors.isEmpty()) {
            return serializedQuery;
        }
        QueryRequest intercepted = interceptorTimer.time(executionContext.contextName(),
                                                         "QueryRequestInterceptor",
                                                         () -> {
                                                             QueryRequest query = serializedQuery.query();
                                                             for (ServiceWithInfo<QueryRequestInterceptor> queryRequestInterceptor : queryRequestInterceptors) {
                                                                 try {
                                                                     query = queryRequestInterceptor.service()
                                                                                                    .queryRequest(query,
                                                                                                                  executionContext);
                                                                 } catch (RequestRejectedException requestRejectedException) {
                                                                     throw new MessagingPlatformException(ErrorCode.QUERY_REJECTED_BY_INTERCEPTOR,
                                                                                                          executionContext
                                                                                                                  .contextName()
                                                                                                                  +
                                                                                                                  ": query rejected by the QueryRequestInterceptor in "
                                                                                                                  + queryRequestInterceptor
                                                                                                                  .pluginKey(),
                                                                                                          requestRejectedException);
                                                                 } catch (Exception interceptorException) {
                                                                     throw new MessagingPlatformException(ErrorCode.EXCEPTION_IN_INTERCEPTOR,
                                                                                                          executionContext
                                                                                                                  .contextName()
                                                                                                                  +
                                                                                                                  ": Exception thrown by the QueryRequestInterceptor in "
                                                                                                                  + queryRequestInterceptor
                                                                                                                  .pluginKey(),
                                                                                                          interceptorException);
                                                                 }
                                                             }
                                                             return query;
                                                         });
        return new SerializedQuery(serializedQuery.context(), serializedQuery.clientStreamId(), intercepted);
    }

    @Override
    public QueryResponse queryResponse(QueryResponse response, ExecutionContext executionContext) {
        List<ServiceWithInfo<QueryResponseInterceptor>> queryResponseInterceptors = pluginContextFilter
                .getServicesWithInfoForContext(
                        QueryResponseInterceptor.class,
                        executionContext.contextName());
        for (ServiceWithInfo<QueryResponseInterceptor> queryResponseInterceptor : queryResponseInterceptors) {
            try {
                response = queryResponseInterceptor.service().queryResponse(response, executionContext);
            } catch (Exception ex) {
                throw new MessagingPlatformException(ErrorCode.EXCEPTION_IN_INTERCEPTOR,
                                                     executionContext.contextName() +
                                                             ": Exception thrown by the QueryResponseInterceptor in "
                                                             + queryResponseInterceptor.pluginKey(),
                                                     ex);
            }
        }
        return response;
    }
}
