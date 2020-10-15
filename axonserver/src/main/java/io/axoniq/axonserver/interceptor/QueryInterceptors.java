/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.interceptor;

import io.axoniq.axonserver.extensions.interceptor.InterceptorContext;
import io.axoniq.axonserver.grpc.SerializedQuery;
import io.axoniq.axonserver.grpc.query.QueryResponse;

/**
 * @author Marc Gathier
 */
public interface QueryInterceptors {

    SerializedQuery queryRequest(InterceptorContext interceptorContext, SerializedQuery serializedQuery);

    QueryResponse queryResponse(InterceptorContext interceptorContext, QueryResponse response);
}
