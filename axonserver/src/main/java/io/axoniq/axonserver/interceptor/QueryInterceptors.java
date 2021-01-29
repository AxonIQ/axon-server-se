/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.interceptor;

import io.axoniq.axonserver.extensions.ExtensionUnitOfWork;
import io.axoniq.axonserver.grpc.SerializedQuery;
import io.axoniq.axonserver.grpc.query.QueryResponse;

/**
 * @author Marc Gathier
 * @since 4.5
 */
public interface QueryInterceptors {

    /**
     * Invokes all {@link io.axoniq.axonserver.extensions.interceptor.QueryRequestInterceptor} instances. Interceptors
     * may change the content of the query request.
     *
     * @param serializedQuery     the query to execute
     * @param extensionUnitOfWork the unit of work for the query
     * @return the query after the interceptors are executed
     */
    SerializedQuery queryRequest(SerializedQuery serializedQuery, ExtensionUnitOfWork extensionUnitOfWork);

    /**
     * Invokes all {@link io.axoniq.axonserver.extensions.interceptor.QueryResponseInterceptor} instances. Interceptors
     * may change the content of the query response. Exceptions in the interceptors are logged, processing continues.
     *
     * @param response            the response of the query
     * @param extensionUnitOfWork the unit of work for the query
     * @return the response after the interceptors are executed
     */
    QueryResponse queryResponse(QueryResponse response, ExtensionUnitOfWork extensionUnitOfWork);
}
