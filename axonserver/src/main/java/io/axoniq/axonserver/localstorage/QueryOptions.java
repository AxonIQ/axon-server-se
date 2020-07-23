/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage;

/**
 * Value object containing options to be used to pre-filter events for ad-hoc queries.
 *
 * @author Marc Gathier
 * @since 4.4
 */
public class QueryOptions {

    private final long minToken;
    private final long maxToken;
    private final long minTimestamp;

    /**
     * @param minToken     minumum token of events to process
     * @param maxToken     maximum token of events to process
     * @param minTimestamp minimum timestamp of events to process
     */
    public QueryOptions(long minToken, long maxToken, long minTimestamp) {
        this.minToken = minToken;
        this.maxToken = maxToken;
        this.minTimestamp = minTimestamp;
    }

    public long getMinToken() {
        return minToken;
    }

    public long getMaxToken() {
        return maxToken;
    }

    public long getMinTimestamp() {
        return minTimestamp;
    }
}
