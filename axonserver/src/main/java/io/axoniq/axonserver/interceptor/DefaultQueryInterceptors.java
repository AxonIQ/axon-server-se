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
import io.axoniq.axonserver.plugin.PluginUnitOfWork;
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
    public SerializedQuery queryRequest(SerializedQuery serializedQuery, PluginUnitOfWork unitOfWork) {
        List<ServiceWithInfo<QueryRequestInterceptor>> queryRequestInterceptors = pluginContextFilter
                .getServicesWithInfoForContext(
                        QueryRequestInterceptor.class,
                        unitOfWork.context());
        if (queryRequestInterceptors.isEmpty()) {
            return serializedQuery;
        }
        QueryRequest intercepted = interceptorTimer.time(unitOfWork.context(),
                                                         "QueryRequestInterceptor",
                                                         () -> {
                                                             QueryRequest query = serializedQuery.query();
                                                             for (ServiceWithInfo<QueryRequestInterceptor> queryRequestInterceptor : queryRequestInterceptors) {
                                                                 try {
                                                                     query = queryRequestInterceptor.service()
                                                                                                    .queryRequest(query,
                                                                                                                  unitOfWork);
                                                                 } catch (RequestRejectedException requestRejectedException) {
                                                                     throw new MessagingPlatformException(ErrorCode.QUERY_REJECTED_BY_INTERCEPTOR,
                                                                                                          unitOfWork
                                                                                                                  .context()
                                                                                                                  +
                                                                                                                  ": query rejected by the QueryRequestInterceptor in "
                                                                                                                  + queryRequestInterceptor
                                                                                                                  .pluginKey(),
                                                                                                          requestRejectedException);
                                                                 } catch (Exception interceptorException) {
                                                                     throw new MessagingPlatformException(ErrorCode.EXCEPTION_IN_INTERCEPTOR,
                                                                                                          unitOfWork
                                                                                                                  .context()
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
    public QueryResponse queryResponse(QueryResponse response, PluginUnitOfWork unitOfWork) {
        List<QueryResponseInterceptor> queryResponseInterceptors = pluginContextFilter.getServicesForContext(
                QueryResponseInterceptor.class,
                unitOfWork.context());
        try {
            for (QueryResponseInterceptor queryResponseInterceptor : queryResponseInterceptors) {
                response = queryResponseInterceptor.queryResponse(response, unitOfWork);
            }
        } catch (Exception ex) {
            logger.warn("{}: an exception occurred in a QueryResponseInterceptor",
                        unitOfWork.context(), ex);
        }
        return response;
    }
}
