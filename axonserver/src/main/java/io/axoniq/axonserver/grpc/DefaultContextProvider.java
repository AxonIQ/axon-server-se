/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.topology.Topology;
import org.springframework.stereotype.Controller;

/**
 * Default implementation of a {@link ContextProvider}. Always returns the default context.
 * @author Marc Gathier
 * @since 4.0
 */
@Controller
public class DefaultContextProvider implements ContextProvider {


    @Override
    public String getContext() {
        return Topology.DEFAULT_CONTEXT;
    }
}
