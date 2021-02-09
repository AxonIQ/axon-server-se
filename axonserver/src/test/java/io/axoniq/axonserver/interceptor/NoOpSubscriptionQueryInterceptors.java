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
import io.axoniq.axonserver.grpc.query.SubscriptionQueryRequest;
import io.axoniq.axonserver.grpc.query.SubscriptionQueryResponse;

/**
 * @author Marc Gathier
 */
public class NoOpSubscriptionQueryInterceptors implements SubscriptionQueryInterceptors {

    @Override
    public SubscriptionQueryRequest subscriptionQueryRequest(SubscriptionQueryRequest subscriptionQueryRequest,
                                                             ExtensionUnitOfWork extensionContext) {
        return subscriptionQueryRequest;
    }

    @Override
    public SubscriptionQueryResponse subscriptionQueryResponse(SubscriptionQueryResponse subscriptionQueryResponse,
                                                               ExtensionUnitOfWork extensionContext) {
        return subscriptionQueryResponse;
    }
}
