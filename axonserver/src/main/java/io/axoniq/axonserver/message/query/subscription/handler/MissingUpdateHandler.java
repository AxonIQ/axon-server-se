/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.query.subscription.handler;

import io.axoniq.axonserver.grpc.query.SubscriptionQueryResponse;
import io.axoniq.axonserver.message.query.subscription.UpdateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Sara Pellegrini on 11/05/2018.
 * sara.pellegrini@gmail.com
 */
public class MissingUpdateHandler implements UpdateHandler {

    private final Logger logger = LoggerFactory.getLogger(MissingUpdateHandler.class);

    @Override
    public void onSubscriptionQueryResponse(SubscriptionQueryResponse response) {
        logger.warn("Cannot find handler for subscriptionId {}. It's not possible to handle SubscriptionQueryResponse.",
                    response.getSubscriptionIdentifier());

    }
}
