/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.query.subscription;

import io.axoniq.axonserver.serializer.Printable;

/**
 * Created by Sara Pellegrini on 18/06/2018.
 * sara.pellegrini@gmail.com
 */
public interface SubscriptionMetrics extends Printable {

    Long totalCount();

    Long activesCount();

    Long updatesCount();

}
