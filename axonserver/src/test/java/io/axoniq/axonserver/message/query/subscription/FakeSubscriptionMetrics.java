/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.query.subscription;

import io.axoniq.axonserver.serializer.Media;

/**
 * Created by Sara Pellegrini on 19/06/2018.
 * sara.pellegrini@gmail.com
 */
public class FakeSubscriptionMetrics implements SubscriptionMetrics{

    private final long totalCount;
    private final long activesCount;
    private final long updatesCount;

    public FakeSubscriptionMetrics(long totalCount, long activesCount, long updatesCount) {
        this.totalCount = totalCount;
        this.activesCount = activesCount;
        this.updatesCount = updatesCount;
    }


    @Override
    public Long totalCount() {
        return totalCount;
    }

    @Override
    public Long activesCount() {
        return activesCount;
    }

    @Override
    public Long updatesCount() {
        return updatesCount;
    }


    @Override
    public void printOn(Media media) {

    }
}