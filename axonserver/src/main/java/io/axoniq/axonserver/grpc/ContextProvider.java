/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.grpc;

/**
 * Provider to return the current context. Default implementation {@link ContextProvider} retrieves the information from GRPC context, other implementations
 * are made in testcases.
 *
 * @author Marc Gathier
 */
public interface ContextProvider {
    String getContext();
}
