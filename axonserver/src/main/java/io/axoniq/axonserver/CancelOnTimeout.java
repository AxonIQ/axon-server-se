/*
 * Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver;

import io.axoniq.axonserver.ActiveRequestsCache.CancelStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * @author Sara Pellegrini
 * @since 4.6.0
 */
public class CancelOnTimeout<T> implements CancelStrategy<T> {

    private final Logger logger = LoggerFactory.getLogger(CancelOnTimeout.class);
    private final String requestType;
    private final Clock clock;
    private final long timeout;
    private final Function<T, String> requestDescription;
    private final Function<T, Long> requestTimestamp;
    private final Consumer<T> cancelRequest;


    public CancelOnTimeout(String requestType, long timeout,
                           Function<T, String> requestDescription,
                           Function<T, Long> requestTimestamp, Consumer<T> cancelRequest) {
        this(requestType, Clock.systemUTC(), timeout, requestDescription, requestTimestamp, cancelRequest);
    }

    public CancelOnTimeout(String requestType, Clock clock, long timeout,
                           Function<T, String> requestDescription,
                           Function<T, Long> requestTimestamp, Consumer<T> cancelRequest) {
        this.requestType = requestType;
        this.clock = clock;
        this.timeout = timeout;
        this.requestDescription = requestDescription;
        this.requestTimestamp = requestTimestamp;
        this.cancelRequest = cancelRequest;
    }


    @Override
    public void cancel(String requestId, T request) {
        if (logger.isWarnEnabled()) {
            logger.warn("Cancelling {} {} - {}", requestType, requestId, requestDescription.apply(request));
        }
        cancelRequest.accept(request);
    }

    @Override
    public boolean requestToBeCanceled(String requestId, T request) {
        long minTimestamp = clock.millis() - timeout;
        return requestTimestamp.apply(request) < minTimestamp;
    }
}
