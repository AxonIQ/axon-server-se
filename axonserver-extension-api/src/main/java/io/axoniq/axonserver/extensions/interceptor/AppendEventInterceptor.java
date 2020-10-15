/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.extensions.interceptor;

import io.axoniq.axonserver.grpc.event.Event;

/**
 * Interceptor that is called when an event is sent to Axon Server. The interceptor is called immediately when
 * the event arrives in Axon Server, before the transaction is committed.
 *
 * @author Marc Gathier
 * @since 4.5
 */
public interface AppendEventInterceptor {

    /**
     * Intercepts an event before it is committed. The interceptor may change the event.
     *
     * @param interceptorContext the context for the request
     * @param event              the new event
     * @return the new event
     */
    Event appendEvent(InterceptorContext interceptorContext, Event event);
}
