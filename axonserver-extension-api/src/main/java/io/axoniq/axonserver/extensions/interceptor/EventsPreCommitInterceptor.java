/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.extensions.interceptor;

/**
 * Interceptor that is called when the client commits a transaction with new events.
 *
 * @author Marc Gathier
 * @since 4.5
 */
public interface EventsPreCommitInterceptor {

    /**
     * Intercepts a transaction before it is committed. The interceptor can no longer
     * change the contents of the transaction.
     *
     * @param interceptorContext the context for the request
     */
    void eventsPreCommit(InterceptorContext interceptorContext);
}
