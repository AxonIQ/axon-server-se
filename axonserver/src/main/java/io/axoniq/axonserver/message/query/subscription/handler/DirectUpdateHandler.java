/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.query.subscription.handler;

import io.axoniq.axonserver.grpc.Publisher;
import io.axoniq.axonserver.grpc.query.SubscriptionQueryResponse;
import io.axoniq.axonserver.message.query.subscription.UpdateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Sara Pellegrini on 03/05/2018.
 * sara.pellegrini@gmail.com
 */
public class DirectUpdateHandler implements UpdateHandler {

    private final Logger logger = LoggerFactory.getLogger(DirectUpdateHandler.class);

    private final Publisher<SubscriptionQueryResponse> destination;

    public DirectUpdateHandler(Publisher<SubscriptionQueryResponse> destination) {
        this.destination = destination;
    }

    @Override
    public void onSubscriptionQueryResponse(SubscriptionQueryResponse response) {
        logger.debug("SubscriptionQueryResponse for subscription Id {} send to client.",
                     response.getSubscriptionIdentifier());
        destination.publish(response);
    }
}
