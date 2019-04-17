/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.query.result;

import java.time.Instant;

/**
 * @author Marc Gathier
 */
public class TimestampExpressionResult extends NumericExpressionResult{

    public TimestampExpressionResult(long value) {
        super(value);
    }

    @Override
    public String toString() {
        return Instant.ofEpochMilli(getNumericValue().longValue()).toString();
    }
}
